package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kelseyhightower/envconfig"
	assistedinstallercontroller "github.com/openshift/assisted-installer/src/assisted_installer_controller"
	"github.com/openshift/assisted-installer/src/inventory_client"
	"github.com/openshift/assisted-installer/src/k8s_client"
	"github.com/openshift/assisted-installer/src/ops"
	"github.com/openshift/assisted-installer/src/utils"
	"github.com/openshift/assisted-service/client/installer"
	"github.com/openshift/assisted-service/models"
	"github.com/openshift/assisted-service/pkg/secretdump"
	"github.com/sirupsen/logrus"
)

var Options struct {
	ControllerConfig assistedinstallercontroller.ControllerConfig
}

const maximumErrorsBeforeExit = 10

func main() {
	logger := logrus.New()

	err := envconfig.Process("myapp", &Options)
	if err != nil {
		log.Fatal(err.Error())
	}

	logger.Infof("Start running Assisted-Controller. Configuration is:\n %s", secretdump.DumpSecretStruct(Options.ControllerConfig))

	kc, err := k8s_client.NewK8SClient("", logger)
	if err != nil {
		log.Fatalf("Failed to create k8 client %v", err)
	}

	err = kc.SetProxyEnvVars()
	if err != nil {
		log.Fatalf("Failed to set env vars for installer-controller pod %v", err)
	}

	client, err := inventory_client.CreateInventoryClient(Options.ControllerConfig.ClusterID,
		Options.ControllerConfig.URL, Options.ControllerConfig.PullSecretToken, Options.ControllerConfig.SkipCertVerification,
		Options.ControllerConfig.CACertPath, logger, utils.ProxyFromEnvVars)
	if err != nil {
		log.Fatalf("Failed to create inventory client %v", err)
	}

	assistedController := assistedinstallercontroller.NewController(logger,
		Options.ControllerConfig,
		ops.NewOps(logger, false),
		client,
		kc,
	)

	// While adding new routine don't miss to add wg.add(1)
	// without adding it will panic
	var wg sync.WaitGroup
	var wgLogs sync.WaitGroup
	var status assistedinstallercontroller.ControllerStatus

	ctxRoutines, cancelRoutines := context.WithCancel(context.Background())
	go assistedController.WaitAndUpdateNodesStatus(ctxRoutines, &wg)
	wg.Add(1)
	go assistedController.PostInstallConfigs(ctxRoutines, &wg, &status)
	wg.Add(1)
	go assistedController.UpdateBMHs(ctxRoutines, &wg)
	wg.Add(1)

	// No need to cancel with context, will finish quickly
	go assistedController.HackDNSAddressConflict(&wg)
	wg.Add(1)

	go assistedController.UploadLogs(ctxRoutines, &wg, &status)
	wgLogs.Add(1)

	assistedController.SetReadyState()

	waitForInstallation(client, logger, &status)
	// stop all go routines
	cancelRoutines()

	logger.Infof("Waiting for all go routines to finish")
	wg.Wait()
	logger.Infof("Waiting for logs to finish")
}

func waitForInstallation(client inventory_client.InventoryClient, log logrus.FieldLogger, status *assistedinstallercontroller.ControllerStatus) {
	log.Infof("monitor cluster installation status")
	reqCtx := utils.GenerateRequestContext()
	errCounter := 0
	for {
		time.Sleep(assistedinstallercontroller.GeneralWaitInterval)
		cluster, err := client.GetCluster(reqCtx)
		if err != nil {
			// In case cluster was deleted or controller is not authorised
			// we should exit controller
			switch err.(type) {
			case *installer.GetClusterNotFound:
				errCounter++
				log.WithError(err).Errorf("Cluster was not found in inventory or user is not authorized")
			case *installer.GetClusterUnauthorized:
				errCounter++
				log.WithError(err).Errorf("User is not authenticated to perform the operation")
			}

			// if we get maximumErrorsBeforeExit errors in a row
			// there is no point to try to reach assisted service
			if errCounter >= maximumErrorsBeforeExit {
				log.Infof("Got more than %d errors from assisted service in a row, exiting", maximumErrorsBeforeExit)
				return
			}
			continue
		}
		errCounter = 0
		switch *cluster.Status {
		case models.ClusterStatusError:
			log.Infof("Cluster installation failed.")
			status.Error()
			return
		case models.ClusterStatusCancelled:
			log.Infof("Cluster installation aborted. Signal the status")
			return
		case models.ClusterStatusInstalled:
			log.Infof("Cluster installation successfully finished.")
			return
		}
	}
}

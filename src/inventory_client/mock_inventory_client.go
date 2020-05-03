// Code generated by MockGen. DO NOT EDIT.
// Source: inventory_client.go

// Package inventory_client is a generated GoMock package.
package inventory_client

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockInventoryClient is a mock of InventoryClient interface.
type MockInventoryClient struct {
	ctrl     *gomock.Controller
	recorder *MockInventoryClientMockRecorder
}

// MockInventoryClientMockRecorder is the mock recorder for MockInventoryClient.
type MockInventoryClientMockRecorder struct {
	mock *MockInventoryClient
}

// NewMockInventoryClient creates a new mock instance.
func NewMockInventoryClient(ctrl *gomock.Controller) *MockInventoryClient {
	mock := &MockInventoryClient{ctrl: ctrl}
	mock.recorder = &MockInventoryClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockInventoryClient) EXPECT() *MockInventoryClientMockRecorder {
	return m.recorder
}

// DownloadFile mocks base method.
func (m *MockInventoryClient) DownloadFile(filename, dest string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadFile", filename, dest)
	ret0, _ := ret[0].(error)
	return ret0
}

// DownloadFile indicates an expected call of DownloadFile.
func (mr *MockInventoryClientMockRecorder) DownloadFile(filename, dest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadFile", reflect.TypeOf((*MockInventoryClient)(nil).DownloadFile), filename, dest)
}

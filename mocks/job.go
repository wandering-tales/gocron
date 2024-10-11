// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/wandering-tales/gocron/v2 (interfaces: Job)
//
// Generated by this command:
//
//	mockgen -destination=mocks/job.go -package=gocronmocks . Job
//
// Package gocronmocks is a generated GoMock package.
package gocronmocks

import (
	reflect "reflect"
	time "time"

	uuid "github.com/google/uuid"
	gomock "go.uber.org/mock/gomock"
)

// MockJob is a mock of Job interface.
type MockJob struct {
	ctrl     *gomock.Controller
	recorder *MockJobMockRecorder
}

// MockJobMockRecorder is the mock recorder for MockJob.
type MockJobMockRecorder struct {
	mock *MockJob
}

// NewMockJob creates a new mock instance.
func NewMockJob(ctrl *gomock.Controller) *MockJob {
	mock := &MockJob{ctrl: ctrl}
	mock.recorder = &MockJobMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockJob) EXPECT() *MockJobMockRecorder {
	return m.recorder
}

// ID mocks base method.
func (m *MockJob) ID() uuid.UUID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(uuid.UUID)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockJobMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockJob)(nil).ID))
}

// LastRun mocks base method.
func (m *MockJob) LastRun() (time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastRun")
	ret0, _ := ret[0].(time.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LastRun indicates an expected call of LastRun.
func (mr *MockJobMockRecorder) LastRun() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastRun", reflect.TypeOf((*MockJob)(nil).LastRun))
}

// Name mocks base method.
func (m *MockJob) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockJobMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockJob)(nil).Name))
}

// NextRun mocks base method.
func (m *MockJob) NextRun() (time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NextRun")
	ret0, _ := ret[0].(time.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NextRun indicates an expected call of NextRun.
func (mr *MockJobMockRecorder) NextRun() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NextRun", reflect.TypeOf((*MockJob)(nil).NextRun))
}

// RunNow mocks base method.
func (m *MockJob) RunNow() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunNow")
	ret0, _ := ret[0].(error)
	return ret0
}

// RunNow indicates an expected call of RunNow.
func (mr *MockJobMockRecorder) RunNow() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunNow", reflect.TypeOf((*MockJob)(nil).RunNow))
}

// Tags mocks base method.
func (m *MockJob) Tags() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tags")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Tags indicates an expected call of Tags.
func (mr *MockJobMockRecorder) Tags() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tags", reflect.TypeOf((*MockJob)(nil).Tags))
}

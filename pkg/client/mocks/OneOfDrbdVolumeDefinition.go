// Code generated by mockery v2.12.3. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// OneOfDrbdVolumeDefinition is an autogenerated mock type for the OneOfDrbdVolumeDefinition type
type OneOfDrbdVolumeDefinition struct {
	mock.Mock
}

// isOneOfDrbdVolumeDefinition provides a mock function with given fields:
func (_m *OneOfDrbdVolumeDefinition) isOneOfDrbdVolumeDefinition() {
	_m.Called()
}

type NewOneOfDrbdVolumeDefinitionT interface {
	mock.TestingT
	Cleanup(func())
}

// NewOneOfDrbdVolumeDefinition creates a new instance of OneOfDrbdVolumeDefinition. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewOneOfDrbdVolumeDefinition(t NewOneOfDrbdVolumeDefinitionT) *OneOfDrbdVolumeDefinition {
	mock := &OneOfDrbdVolumeDefinition{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

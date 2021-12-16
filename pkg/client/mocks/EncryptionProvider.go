// Code generated by mockery v2.9.4. DO NOT EDIT.

package mocks

import (
	context "context"

	client "github.com/LINBIT/golinstor/client"

	mock "github.com/stretchr/testify/mock"
)

// EncryptionProvider is an autogenerated mock type for the EncryptionProvider type
type EncryptionProvider struct {
	mock.Mock
}

// Create provides a mock function with given fields: ctx, passphrase
func (_m *EncryptionProvider) Create(ctx context.Context, passphrase client.Passphrase) error {
	ret := _m.Called(ctx, passphrase)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, client.Passphrase) error); ok {
		r0 = rf(ctx, passphrase)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Enter provides a mock function with given fields: ctx, password
func (_m *EncryptionProvider) Enter(ctx context.Context, password string) error {
	ret := _m.Called(ctx, password)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, password)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Modify provides a mock function with given fields: ctx, passphrase
func (_m *EncryptionProvider) Modify(ctx context.Context, passphrase client.Passphrase) error {
	ret := _m.Called(ctx, passphrase)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, client.Passphrase) error); ok {
		r0 = rf(ctx, passphrase)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

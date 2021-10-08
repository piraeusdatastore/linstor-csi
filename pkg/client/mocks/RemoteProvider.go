// Code generated by mockery v2.8.0. DO NOT EDIT.

package mocks

import (
	context "context"

	client "github.com/LINBIT/golinstor/client"

	mock "github.com/stretchr/testify/mock"
)

// RemoteProvider is an autogenerated mock type for the RemoteProvider type
type RemoteProvider struct {
	mock.Mock
}

// CreateLinstor provides a mock function with given fields: ctx, create
func (_m *RemoteProvider) CreateLinstor(ctx context.Context, create client.LinstorRemote) error {
	ret := _m.Called(ctx, create)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, client.LinstorRemote) error); ok {
		r0 = rf(ctx, create)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateS3 provides a mock function with given fields: ctx, create
func (_m *RemoteProvider) CreateS3(ctx context.Context, create client.S3Remote) error {
	ret := _m.Called(ctx, create)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, client.S3Remote) error); ok {
		r0 = rf(ctx, create)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, remoteName
func (_m *RemoteProvider) Delete(ctx context.Context, remoteName string) error {
	ret := _m.Called(ctx, remoteName)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, remoteName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAll provides a mock function with given fields: ctx, opts
func (_m *RemoteProvider) GetAll(ctx context.Context, opts ...*client.ListOpts) (client.RemoteList, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 client.RemoteList
	if rf, ok := ret.Get(0).(func(context.Context, ...*client.ListOpts) client.RemoteList); ok {
		r0 = rf(ctx, opts...)
	} else {
		r0 = ret.Get(0).(client.RemoteList)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllLinstor provides a mock function with given fields: ctx, opts
func (_m *RemoteProvider) GetAllLinstor(ctx context.Context, opts ...*client.ListOpts) ([]client.LinstorRemote, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []client.LinstorRemote
	if rf, ok := ret.Get(0).(func(context.Context, ...*client.ListOpts) []client.LinstorRemote); ok {
		r0 = rf(ctx, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]client.LinstorRemote)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllS3 provides a mock function with given fields: ctx, opts
func (_m *RemoteProvider) GetAllS3(ctx context.Context, opts ...*client.ListOpts) ([]client.S3Remote, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []client.S3Remote
	if rf, ok := ret.Get(0).(func(context.Context, ...*client.ListOpts) []client.S3Remote); ok {
		r0 = rf(ctx, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]client.S3Remote)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ModifyLinstor provides a mock function with given fields: ctx, remoteName, modify
func (_m *RemoteProvider) ModifyLinstor(ctx context.Context, remoteName string, modify client.LinstorRemote) error {
	ret := _m.Called(ctx, remoteName, modify)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, client.LinstorRemote) error); ok {
		r0 = rf(ctx, remoteName, modify)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ModifyS3 provides a mock function with given fields: ctx, remoteName, modify
func (_m *RemoteProvider) ModifyS3(ctx context.Context, remoteName string, modify client.S3Remote) error {
	ret := _m.Called(ctx, remoteName, modify)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, client.S3Remote) error); ok {
		r0 = rf(ctx, remoteName, modify)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

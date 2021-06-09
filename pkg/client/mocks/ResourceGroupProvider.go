// Code generated by mockery v2.8.0. DO NOT EDIT.

package mocks

import (
	context "context"

	client "github.com/LINBIT/golinstor/client"

	mock "github.com/stretchr/testify/mock"
)

// ResourceGroupProvider is an autogenerated mock type for the ResourceGroupProvider type
type ResourceGroupProvider struct {
	mock.Mock
}

// Create provides a mock function with given fields: ctx, resGrp
func (_m *ResourceGroupProvider) Create(ctx context.Context, resGrp client.ResourceGroup) error {
	ret := _m.Called(ctx, resGrp)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, client.ResourceGroup) error); ok {
		r0 = rf(ctx, resGrp)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateVolumeGroup provides a mock function with given fields: ctx, resGrpName, volGrp
func (_m *ResourceGroupProvider) CreateVolumeGroup(ctx context.Context, resGrpName string, volGrp client.VolumeGroup) error {
	ret := _m.Called(ctx, resGrpName, volGrp)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, client.VolumeGroup) error); ok {
		r0 = rf(ctx, resGrpName, volGrp)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, resGrpName
func (_m *ResourceGroupProvider) Delete(ctx context.Context, resGrpName string) error {
	ret := _m.Called(ctx, resGrpName)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, resGrpName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteVolumeGroup provides a mock function with given fields: ctx, resGrpName, volNr
func (_m *ResourceGroupProvider) DeleteVolumeGroup(ctx context.Context, resGrpName string, volNr int) error {
	ret := _m.Called(ctx, resGrpName, volNr)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, int) error); ok {
		r0 = rf(ctx, resGrpName, volNr)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: ctx, resGrpName, opts
func (_m *ResourceGroupProvider) Get(ctx context.Context, resGrpName string, opts ...*client.ListOpts) (client.ResourceGroup, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, resGrpName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 client.ResourceGroup
	if rf, ok := ret.Get(0).(func(context.Context, string, ...*client.ListOpts) client.ResourceGroup); ok {
		r0 = rf(ctx, resGrpName, opts...)
	} else {
		r0 = ret.Get(0).(client.ResourceGroup)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, resGrpName, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAll provides a mock function with given fields: ctx, opts
func (_m *ResourceGroupProvider) GetAll(ctx context.Context, opts ...*client.ListOpts) ([]client.ResourceGroup, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []client.ResourceGroup
	if rf, ok := ret.Get(0).(func(context.Context, ...*client.ListOpts) []client.ResourceGroup); ok {
		r0 = rf(ctx, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]client.ResourceGroup)
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

// GetPropsInfos provides a mock function with given fields: ctx, opts
func (_m *ResourceGroupProvider) GetPropsInfos(ctx context.Context, opts ...*client.ListOpts) error {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, ...*client.ListOpts) error); ok {
		r0 = rf(ctx, opts...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetVolumeGroup provides a mock function with given fields: ctx, resGrpName, volNr, opts
func (_m *ResourceGroupProvider) GetVolumeGroup(ctx context.Context, resGrpName string, volNr int, opts ...*client.ListOpts) (client.VolumeGroup, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, resGrpName, volNr)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 client.VolumeGroup
	if rf, ok := ret.Get(0).(func(context.Context, string, int, ...*client.ListOpts) client.VolumeGroup); ok {
		r0 = rf(ctx, resGrpName, volNr, opts...)
	} else {
		r0 = ret.Get(0).(client.VolumeGroup)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, int, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, resGrpName, volNr, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetVolumeGroupPropsInfos provides a mock function with given fields: ctx, resGrpName, opts
func (_m *ResourceGroupProvider) GetVolumeGroupPropsInfos(ctx context.Context, resGrpName string, opts ...*client.ListOpts) error {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, resGrpName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...*client.ListOpts) error); ok {
		r0 = rf(ctx, resGrpName, opts...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetVolumeGroups provides a mock function with given fields: ctx, resGrpName, opts
func (_m *ResourceGroupProvider) GetVolumeGroups(ctx context.Context, resGrpName string, opts ...*client.ListOpts) ([]client.VolumeGroup, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, resGrpName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []client.VolumeGroup
	if rf, ok := ret.Get(0).(func(context.Context, string, ...*client.ListOpts) []client.VolumeGroup); ok {
		r0 = rf(ctx, resGrpName, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]client.VolumeGroup)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, ...*client.ListOpts) error); ok {
		r1 = rf(ctx, resGrpName, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Modify provides a mock function with given fields: ctx, resGrpName, props
func (_m *ResourceGroupProvider) Modify(ctx context.Context, resGrpName string, props client.ResourceGroupModify) error {
	ret := _m.Called(ctx, resGrpName, props)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, client.ResourceGroupModify) error); ok {
		r0 = rf(ctx, resGrpName, props)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ModifyVolumeGroup provides a mock function with given fields: ctx, resGrpName, volNr, props
func (_m *ResourceGroupProvider) ModifyVolumeGroup(ctx context.Context, resGrpName string, volNr int, props client.VolumeGroupModify) error {
	ret := _m.Called(ctx, resGrpName, volNr, props)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, int, client.VolumeGroupModify) error); ok {
		r0 = rf(ctx, resGrpName, volNr, props)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Spawn provides a mock function with given fields: ctx, resGrpName, resGrpSpwn
func (_m *ResourceGroupProvider) Spawn(ctx context.Context, resGrpName string, resGrpSpwn client.ResourceGroupSpawn) error {
	ret := _m.Called(ctx, resGrpName, resGrpSpwn)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, client.ResourceGroupSpawn) error); ok {
		r0 = rf(ctx, resGrpName, resGrpSpwn)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

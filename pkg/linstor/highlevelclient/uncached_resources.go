/*
CSI Driver for Linstor
Copyright © 2019 LINBIT USA, LLC

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

package highlevelclient

import (
	"context"

	lapi "github.com/LINBIT/golinstor/client"
)

type uncachedResourcesKey struct{}

// WithUncachedResources returns a context for which resource queries skip the client side resource cache.
//
// The cache is only invalidated by modifications made through the client. Resources that LINSTOR creates on its own,
// for example the replicas of a cloned resource definition, are missing from it until it expires.
func WithUncachedResources(ctx context.Context) context.Context {
	return context.WithValue(ctx, uncachedResourcesKey{}, true)
}

func uncachedResources(ctx context.Context) bool {
	bypass, _ := ctx.Value(uncachedResourcesKey{}).(bool)
	return bypass
}

// uncachedResourceProvider sends resource queries directly to LINSTOR if the context was derived from
// WithUncachedResources. All other calls, including all modifications, go through the wrapped provider.
type uncachedResourceProvider struct {
	lapi.ResourceProvider
	// uncached is the provider that the resource cache wraps.
	uncached lapi.ResourceProvider
}

var _ lapi.ResourceProvider = &uncachedResourceProvider{}

func (p *uncachedResourceProvider) by(ctx context.Context) lapi.ResourceProvider {
	if uncachedResources(ctx) {
		return p.uncached
	}

	return p.ResourceProvider
}

func (p *uncachedResourceProvider) GetResourceView(ctx context.Context, opts ...*lapi.ListOpts) ([]lapi.ResourceWithVolumes, error) {
	return p.by(ctx).GetResourceView(ctx, opts...)
}

func (p *uncachedResourceProvider) GetAll(ctx context.Context, resName string, opts ...*lapi.ListOpts) ([]lapi.Resource, error) {
	return p.by(ctx).GetAll(ctx, resName, opts...)
}

func (p *uncachedResourceProvider) Get(ctx context.Context, resName, nodeName string, opts ...*lapi.ListOpts) (lapi.Resource, error) {
	return p.by(ctx).Get(ctx, resName, nodeName, opts...)
}

func (p *uncachedResourceProvider) GetVolumes(ctx context.Context, resName, nodeName string, opts ...*lapi.ListOpts) ([]lapi.Volume, error) {
	return p.by(ctx).GetVolumes(ctx, resName, nodeName, opts...)
}

func (p *uncachedResourceProvider) GetVolume(ctx context.Context, resName, nodeName string, volNr int, opts ...*lapi.ListOpts) (lapi.Volume, error) {
	return p.by(ctx).GetVolume(ctx, resName, nodeName, volNr, opts...)
}

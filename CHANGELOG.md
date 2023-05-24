# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- When using another property namespace (other than `Aux/`), also use it to prefix `replicasOnSame`
  and `replicasOnDifferent`. Values already prefixed with `Aux/` will be left unchanged.
- Starting with 0.13.1, the CSI driver tried to work around an issue in quorum calculation
  by forcing the creation of a diskful instead of the normal diskless resource. Since DRBD 9.1.12/9.2.0,
  this is no longer necessary, as quorum is now inherited from the diskful counterparts.

## [1.0.1] - 2023-04-24

### Added

- Ignore storage capacity from "unschedulable" nodes. Those are nodes that are either evicted/evacuated or have the
  `AutoplaceTarget` property set to `false`.

## [1.0.0] - 2023-02-28

### Changed

- Add `_netdev` mount option to all mounts. This ensures unmounting on system shutdown happens before the
  network is unconfigured, which might cause DRBD to lose quorum.

## [0.22.1] - 2022-12-27

### Changed

- Use golinstor based initialization of LINSTOR client. This enables checking the server TLS without requiring a
  mutual TLS config, as well as using the new built-in User-Agent and Bearer Token configs.

## [0.22.0] - 2022-12-13

### Added

- Support for live-migration in kubevirt by setting the `allow-two-primaries` property on demand. This only affects
  RWX volumes in block mode. If used for anything other than kube-virt, results cannot be guaranteed.

### Fixed

- LINSTOR may return more backups than requested when searching for a specific snapshot ID. The results are now
  filtered, ensuring we really restore from the expected backup.

## [0.21.0] - 2022-10-21

### Added

- Add option to set property namespace used to determine a nodes topology.
- Add option to skip labelling nodes based on configured storage pools.

## [0.20.0] - 2022-07-20

### Added

- A new check ensures snapshots are only attempted when the resource is completely in sync. This prevents an issue
  with LINSTOR creating too many error reports in case a node is (temporarily) offline.
- Support CSI Volume Conditions

### Changed

- Update CSI Spec to 1.6.0

## [0.19.1] - 2022-05-24

### Added

- Store access policy in volume context, enabling volume context parsing without access to the storage class.
  This improves the situation for LINSTOR Scheduler and Affinity Controller, which need to access this value.

### Changed

- The attach logic was updated to take the quorum configuration into account. Previously, the CSI driver would
  create a diskful resource in the assumption that it is required to achieve quorum, even if the cluster was
  configured without quorum.

## [0.19.0] - 2022-05-09

### Added

- Snapshots from S3 are now restored by preferred topology, if possible. Previously, all admissible nodes based
  on the provided cluster topology were considered in random order. Now the order is fixed to be based on the
  preferred topology instead.

### Breaking

- An empty filesystem volume that was provisioned with 0.18.0, but never attached will not be attachable. It is missing
  the filesystem, since LINSTOR CSI no longer creates it on attach. You have to recreate these volumes (by definition,
  they hold no data).

### Changed

- Use LINSTOR native properties to provision filesystem volumes. This might slightly alter the default options used
  when running `mkfs`, LINSTOR choses defaults optimized for DRBD. Previously filesystem creation was part of the usual
  mount process. This could run into timeouts for very large volumes: as the process would be restarted from scratch
  on every timeout, it would never complete.

## [0.18.0] - 2022-02-24

### Added

- A new parameter `usePvcName` was added. When "true", the driver will (try to) use the PVC name + namespace instead
  of the generated PV name as ID. Because this might generate name conflicts in certain scenarios, it remains disabled
  by default.
- Create backups of volumes, stored on S3 compatible storage. See
  [`examples/k8s/volume-snapshot-class.yaml`](./examples/k8s/volume-snapshot-class.yaml) for how backups can be
  configured. Note: restoring from such a backup to a new cluster requires some manual steps. Check the examples.
- Include [`linstor-wait-until`] in the image, to make it easy to wait for the LINSTOR API before starting the driver.

[`linstor-wait-until`]: https://github.com/LINBIT/linstor-wait-until

### Changed

- Snapshots are only restored on one node, then replicas are distributed using the usual scheduler logic. Previously
  a volume restored from a snapshot would be deployed on exactly the same nodes as the original.
- Require golang 1.17 for `go generate`, removing the binary dependencies from go.mod.
- Update CSI Spec to 1.5.0

### Fixed

- ControllerUnpublishVolume could fail if LINSTOR node was deleted before the detach operation was executed.
- LINSTOR CSI now expands a restored or cloned volume (if required) before deploying to multiple nodes. A single
  resource should always be in sync, so it is always safe to expand them.

## [0.17.0] - 2021-12-09

### Added

- `allowRemoteVolumeAccess` supports new values, allowing a more fine-grained controlled on diskless access.

### Fixed

- ControllerPublishVolume will attach a new volume on the selected node, even if it violates the replicasOn...
  parameters. This prevented `allowRemoteVolumeAccess` from working as expected.

## [0.16.1] - 2021-11-08

### Changed

- When using a volume source (snapshot or other volume), LINSTOR CSI now correctly resizes the volume to the new
  requested size.
- Switch back to `resource definition(rd) -> volume definition(vd) -> autoplace(ap)` provisioning instead of
  `rd -> ap-> vd` as introduced in 0.16.0. This caused issues on non-thin storage pools and sometimes provisioning
  failed even on thin storage pools.

## [0.16.0] - 2021-10-15

### Changed

- Previous versions of LINSTOR CSI passed parameters (as set in a storage class for example) via custom properties on
  the LINSTOR resources. If those properties were not found, the volume could not be mounted. This made is hard to
  use pre-provisioned volumes, for example those restored from off-site backups. Now the parameters are passed via
  the CSI native "Volume Context", which makes it possible to use pre-provisioned volumes.

## [0.15.1] - 2021-10-06

### Fixed

- A bug introduced in 0.14.0 meant that using the "FollowTopology" policy would not create the requested amount
  of volume replicas. Instead, only a single replica was created. This bug is now fixed. Existing volumes can be
  updated by using `linstor rg adjust <resource group>`.

## [0.15.0] - 2021-09-23

### Added

- New default volume scheduler `AutoPlaceTopology`. This new scheduler is a topology aware version of the old
  `AutoPlace` scheduler. Since it is topology aware, it can be used to optimize volume placement when using
  `WaitForFirstConsumer` volume binding or restricting placement via `allowedTopologies`, while still respecting
  user-defined placement options such as `replicasOnSame` or `replicasOnDifferent`.

## [0.14.1] - 2021-09-02

### Added

- Build image for arm64

## [0.14.0] - 2021-08-18

### Added

- Option to send a bearer token for authentication. This can be used when the API is secured by a project like
  `kube-rbac-proxy`.
- Add explicit namespace `linstor.csi.linbit.com` to volume parameters in storage class. This makes it easier to track
  which parameter is handled by which component (for example: `csi.storage.k8s.io/fstype` is handled by the CSI
  infrastructure, not the plugin itself).

  Un-namespaced parameters are still supported, while explicitly namespaced
  parameters that with a foreign namespace are now ignored. These would produce "unknown parameter" errors previously.

### Changed

- Calls to `GetCapacity` now take topology information (if any) into account.

## [0.13.1] - 2021-06-10

### Added

- Try to detect cases where a diskless resource would not get quorum and deploy a diskfull replica instead.
  [#121](https://github.com/piraeusdatastore/linstor-csi/pull/121)
- Add deactivate/active logic when using resources on shared storage.

## [0.13.0] - 2021-05-12

### Added
- Allow setting arbitrary properties using a parameter prefixed with the `property.linstor.csi.linbit.com` namespace.

### Changed
- Generate a resource group name if non was provided. The name is generated based on the provided parameters. Since
  storage classes are immutable, volumes provisioned using the same storage class will always receive the same
  resource group name.

### Removed
- Resource groups no longer update existing properties or remove additional properties if they not set in the storage
  class. A resource group is immutable from the linstor-csi's point of view.

### Fixed
- Failed snapshots are now cleaned up and retried properly. This mitigates an issue whereby the snapshot failed for
  one reason or other, but the snapshot controller contiously polls it for "completion".

## [0.12.1] - 2021-03-31

### Fixed
- Controller no longer treats LINSTOR resource(-definitions) as ready when the DELETE flag is set. Operations are now
  aborted early and tried again later when the LINSTOR resource is really gone.

## [0.12.0] - 2021-01-26

### Changed
- PVCs can now be deleted independently of Snapshots. LINSTOR ResourceDefinitions for the PVC will exist until
  both Snapshots and Resources are deleted.

### Fixed
- Add `nouuid` to default XFS mount options. This enables mounting restored snapshots on the same node as the original.
- Detach() operations no longer delete existing diskless resources (i.e. TieBreaker resources), only those created by
  Attach() operations.

## [0.11.0] - 2020-12-21

### Changed
- Snapshot information is persisted using native LINSTOR Snapshots instead of storing it in properties of RDs.
- Snapshots are marked as ready only after LINSTOR reports success
- Generate fallback id for a snapshot based on the suggested name using UUIDv5
- Only create a single snapshot in volume-from-volume scenarios

### Fixed
- LayerList was ignored when not using the AutoPlace scheduler. All schedulers not pass this information to LINSTOR. [#102]

[#102]: https://github.com/piraeusdatastore/linstor-csi/issues/102

## [0.10.2] - 2020-12-04

### Fixed
- Crash when calling NodePublishVolume on non-existent volume ([#96])
- Fix an issue where newly created volumes would not be placed on any nodes, leaving them unusable ([#99])

[#96]: https://github.com/piraeusdatastore/linstor-csi/issues/96
[#99]: https://github.com/piraeusdatastore/linstor-csi/issues/99

## [0.10.1] - 2020-11-19

### Changed
- ROX block volumes support requires DRBD v9.0.26 ([#93])

[#93]: https://github.com/piraeusdatastore/linstor-csi/pull/93

## [0.10.0] - 2020-11-11

### Added
- Support for ROX filesystem and block volumes ([#87] and [#88])
- Support all currently available LINSTOR layers
  - DRBD
  - STORAGE
  - LUKS
  - NVME
  - CACHE (new)
  - OPENFLEX (new)
  - WRITECACHE (new)

### Changed
- Use storage pools for CSI topology support. ([#83])
- `replicasOnSame` and `replicasOnDifferent` always use auxiliary properties

[#88]: https://github.com/piraeusdatastore/linstor-csi/pull/88
[#87]: https://github.com/piraeusdatastore/linstor-csi/pull/87
[#83]: https://github.com/piraeusdatastore/linstor-csi/pull/83

## [0.9.1] - 2020-07-28
### Fixed
- "layerlist" is respected when auto-placing volumes on older LINSTOR versions. ([#77])

[#77]: https://github.com/piraeusdatastore/linstor-csi/issues/77

## [0.9.0] - 2020-06-15
### Added
- support `VolumeExpand` capabilities

### Changed
- Updated CSI spec to v1.2.0

## [0.8.3] - 2020-06-08
### Fixed
- PVC from snapshots mount the restored volume instead of the original

## [0.8.2] - 2020-06-04
### Fixed
- Snapshot/restore via CSI external-snapshotter

## [0.8.1] - 2020-05-14
### Fixed
- allow empty/unset LINSTOR storage pools, LINSTOR will chose one. Do not set "", which breaks

## [0.8.0] - 2020-04-16
### Changed
- moved upstream to [piraeus](https://github.com/piraeusdatastore/linstor-csi)
- Licence changed to Apache2 (from GPLv2)
### Deprecated
- SCs without a `resourceGroup` parameter. For now we create a random LINSTOR RG for every PVC of a SC that does not specify `resourceGroup`. You should get rid of these SCs soon.
### Added
- TLS mutual auth support for LINSTOR API endpoint
- support for LINSTOR resource groups. Every storage class mapps now to a LINSTOR resource group.
- DRBD options can now be specified in the SC as "parameter". See the example in `class.yaml`. Keys as specified in the LINSTOR REST API.

## [0.7.4] - 2020-02-27
### Changed
- disable host network
### Fixed
- do not cosider diskless storage pools for placement in `Balanced` placer
### Added
- implement NodeGetVolumeStats
- update to recent tool chain

## [0.7.3] - 2020-01-07
### Added
- postMountXfsOpts parameter in a storageclass spec can specify the parameter for an xfs_io called
  after mounting an XFS volume
- new  `placementPolicy`:
  - `Balanced` provisions remote volumes in the same `failure-domain.beta.kubernetes.io/zone`, picks least utilized `StoragePool`, node and `PrefNic` calculated as `(total_capacity - free_capacity) / total_capacity`<!-- Needs Docs -->

## [0.7.2] - 2019-08-09
### Added
- `linstor-skip-tls-verification` argument for csi-plugin. Set to `"true"` to
  disable tls verification<!-- Needs Docs -->
- csi-plugin will read `LS_USERNAME` and `LS_PASSWORD` environment variables for
  https auth credentials<!-- Needs Docs -->
### Changed
- Upgrade to golinstor v0.16.1 from v0.16.2

## [0.7.1] - 2019-08-08
### Changed
- better logging for Mount calls.
- csi-plugin base imagine is now debian:buster was alpine

### Fixed
- filesystem options no longer ignored. Introduced in v0.6.0

## [0.7.0] - 2019-08-07
### Added
- `placementPolicy` parameter to control where volumes are physically placed
on storage with the following (case sensitive!) options:<!-- Needs Docs -->
  - `AutoPlace` uses LINSTOR autoplace<!-- Needs Docs -->
  - `Manual` uses `clientList` and `nodeList`<!-- Needs Docs -->
  - `FollowTopology` attempts to provision volumes according to volume
  topology preferences `placementCount` number of times.<!-- Needs Docs -->
- `allowRemoteVolumeAccess` parameter which allow volumes to be remotely attached
  to nodes (diskless). Defaults to `"true"`<!-- Needs Docs -->
- `placementCount` parameter to determine how many replicas to create. Alias to
  the `autoPlace` parameter<!-- Needs Docs -->
### Removed
- localStoragePolicy parameter<!-- Needs Docs -->
### Changed
- Upgrade to golinstor v0.16.1 from v0.15.0

## [0.6.4] - 2019-06-25
### Fixed
- attach diskless resources, rather than diskful. introduced in 0.6.3

## [0.6.3] - 2019-06-24
### Added
- `localStoragePolicy` now accepts `prefer` and `require` These are now the
  preferred (but not required) way to specify these options. <!-- Needs Docs -->
### Fixed
- multivolume resources were potentially broken, but this plugin only makes
  single volume resources, so this was a corner case.
- getting volume names was broken if there were non-csi-annotated resource
  definitions

## [0.6.2] - 2019-06-17
### Added
- disklessly attached volumes report volume topology based off of
  `disklessStoragePool` parameter. By default, they are available on all LINSTOR
  nodes. Creating new diskless storage pools on a subset of nodes can be used to
  control from where diskless volumes can be accessed. <!-- Needs Docs -->
- support for raw block volumes <!-- Needs Docs -->
- `linstor-api-requests-per-second` plugin argument <!-- Needs Docs -->
- `linstor-api-burst` plugin argument <!-- Needs Docs -->

### Removed
- NodeStageVolume and NodeUnstageVolume, everything can be done via
  NodePublishVolume and NodeUnpublishVolume

### Fixed
- ListVolumes now working

### Changed
- Updated dependency versions

## [0.6.1] - 2019-06-12
### Fixed
- `localStoragePolicy` parameter now case insensitive when reporting volume topology,
  previously needed to be lowercase

## [0.6.0] - 2019-06-11
### Added
- log-level argument for csi-plugin. it takes strings of levels: info, debug,
  error, etc. The help text lists all of them, but those are the only three you
  need. Defaults to info.
- support for diskless on remaining is added as a storage class parameter
  example `disklessOnRemaining: true` defaults to false <!-- Needs Docs -->
- minimal node health checking during relevant CSI calls
- ListVolumes call implemented
- GetCapacity call implemented
- publishing READONLY supported
- LINSTOR api calls are now subject to csi sidecar container timeouts. Timeout
  argument values of these containers may need to be lengthened, see example deployment
- CHANGELOG.md (so meta!)

### Removed
- debug-logging argument for csi-plugin
- force and blocksize parameters: use fsOpts for these

### Fixed
- resource definitions should be cleaned up in all cases where CreateVolume calls fail
- mounting now checks for plugin's ability to do an exclusive open on
  the resources backing device, addresses https://github.com/LINBIT/linstor-csi/issues/15

### Changed
- controllers argument for csi-plugin is now linstor-endpoint and must
  be a url with a protocol (http://...) this now points to the linstor
  rest api work for LINSTOR controller 0.9.11 or better. <!-- Needs Docs -->
- endpoint argument for csi-plugin is now csi-endpoint
- examples/k8s/deploy/linstor-csi.yaml updated
- csi-plugin base imagine is now alpine: was linstor-client
- golinstor is bumped to version v0.15.0 and uses the rest api exclusively
- storagePool now defaults to "", rather than DfltStorPool allowing LINSTOR
  to choose storage pools on pure autoPlace calls. This is not supported
  in conjunction with localStoragePolicies `required` or `preferred` <!-- Needs Docs -->
- encryption parameter takes boolean values, like `disklessOnRemaining`: <!-- Needs Docs -->
  - 1, t, T, TRUE, true, and True
  - 0, f, F, FALSE, false, and False
- all storageClass parameters options are now case insenstive
- non-debug logging is less verbose in general

[Unreleased]: https://github.com/piraeusdatastore/linstor-csi/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.22.1...v1.0.0
[0.22.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.22.0...v0.22.1
[0.22.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.21.0...v0.22.0
[0.21.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.20.0...v0.21.0
[0.20.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.19.1...v0.20.0
[0.19.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.19.0...v0.19.1
[0.19.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.16.1...v0.17.0
[0.16.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.16.0...v0.16.1
[0.16.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.15.1...v0.16.0
[0.15.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.15.0...v0.15.1
[0.15.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.14.1...v0.15.0
[0.14.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.13.1...v0.14.0
[0.13.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.10.2...v0.11.0
[0.10.2]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.8.3...v0.9.0
[0.8.3]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.7.4...v0.8.0
[0.7.4]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.6.4...v0.7.0
[0.6.4]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/piraeusdatastore/linstor-csi/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/piraeusdatastore/linstor-csi/tree/v0.6.0

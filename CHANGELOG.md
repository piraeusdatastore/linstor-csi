# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- PVCs can now be deleted independently of Snapshots. LINSTOR ResourceDefinitions for the PVC will exist until
  both Snapshots and Resources are deleted.

### Fixed
- Add `nouuid` to default XFS mount options. This enables mounting restored snapshots on the same node as the original.

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

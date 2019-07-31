# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2019-08-19
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

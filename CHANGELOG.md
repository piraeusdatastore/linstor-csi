# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2019-06-20
### Fixed
- `localStoragePolicy` parameter now case insensitive when reporting volume topology,
  previously needed to be lowercase

## [0.6.0] - 2019-06-19
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

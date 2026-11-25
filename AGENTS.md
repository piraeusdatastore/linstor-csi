# Agent instructions

This repository contains the LINSTOR CSI driver.
The driver provides LINSTOR volumes to container orchestrators that implement CSI, such as Kubernetes.
[CONTRIBUTING.md](./CONTRIBUTING.md) describes the contribution workflow.
Follow it.
The following points cover the steps that automated changes most often get wrong.

## Commands

```sh
go build ./cmd/linstor-csi                      # build the driver
go test ./...                                   # unit tests
go test ./pkg/volume/ -run TestParseSnapshotId  # a single test
make lint                                       # golangci-lint, pinned version
bin/golangci-lint fmt                           # apply gofumpt and gci, after make lint
go generate ./...                               # regenerate enumer and mockery files
```

`make sanity` and `make sanity-cg` run the driver against a real LINSTOR cluster.
They use `sudo`, `lvm2`, and Docker or Podman.
They create a loopback LVM thin pool on the host and run the driver as root.
Do not run them unless you are asked to.

## Generated files

Do not edit generated files by hand.
Change the source, then run `go generate ./...`:

* `*_enumer.go`: generated from the `PlacementPolicy`, `paramKey`, and `SnapshotType` constants.
* `pkg/client/mocks/`: generated from the golinstor client interfaces.
  Regenerate it after you update `github.com/LINBIT/golinstor`.

Commit the regenerated files with the change that caused them.

## Compatibility

* Add new storage class parameters as optional, and keep the previous behavior when they are not set.
* Do not remove or rename parameters, or change their meaning.
* Do not change the format of volume or snapshot IDs.
  Existing `PersistentVolume` and `VolumeSnapshotContent` resources store them.

## Code

* Return errors from CSI RPCs as `google.golang.org/grpc/status` errors with the matching `codes` value.
  The CSI sanity suite checks the codes.
* Group imports as standard library, third party, then `github.com/piraeusdatastore/linstor-csi`.
  `bin/golangci-lint fmt` sorts them.

## Changelog

Add an entry under `[Unreleased]` in `CHANGELOG.md` for every change that affects users of the driver.
Describe the effect on the user in one or two lines.
Changes to documentation, CI, or tests do not get an entry.

## Commits

* Write the subject line in the imperative mood, without a prefix.
* Sign off every commit with `git commit -s`.
* During a review, fold follow-up fixes into the commits that introduced the problem with `git commit --fixup` and `git rebase --autosquash`.

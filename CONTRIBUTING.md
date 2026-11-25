# Contributing to LINSTOR CSI

This guide describes the workflow for contributing to the LINSTOR CSI driver.
For an overview of the project, read the [README](./README.md) first.

## Before you start

Look through the [issues](https://github.com/piraeusdatastore/linstor-csi/issues) for something to work on.
To contribute something that has no issue yet, open an issue first to discuss it.

## Target branch

Open pull requests against the `master` branch.

## Development tools

* [Go](https://go.dev/), in the version declared in `go.mod`.
* [`pre-commit`](https://pre-commit.com/) runs the checks described in [Commit hooks](#commit-hooks).
* [`golangci-lint`](https://github.com/golangci/golangci-lint) lints Go code and formats it with `gofumpt` and `gci`.
  Run it with `make lint`, which downloads the version pinned in `.golangci-lint-version` to `bin/`.
  CI uses the same version.
* [Docker Buildx](https://docs.docker.com/build/buildx/), which `make upload` uses to build the container images.
* `sudo`, `lvm2`, and Docker or Podman, which the [sanity tests](#sanity-tests) use.

`enumer`, `mockery`, and `csi-sanity` are declared as `tool` dependencies in `go.mod`.
`go tool` and `go generate` build them on demand.

## Generated files

The following files are generated.
Do not edit them by hand:

| Generated files                  | Source                                                       | Regenerate with      |
|----------------------------------|--------------------------------------------------------------|----------------------|
| `*_enumer.go`                    | `PlacementPolicy`, `paramKey`, and `SnapshotType` constants  | `go generate ./...`  |
| `pkg/client/mocks/`              | The golinstor client interfaces, `pkg/client/.mockery.yaml`  | `go generate ./...`  |

Run `go generate ./...` after you add a constant to one of these types, or after you update `github.com/LINBIT/golinstor`.
Commit the regenerated files together with the change that caused them.

## Storage class parameters

Storage classes in existing clusters must keep working after an upgrade:

* Add new parameters as optional.
  If a new parameter is not set, the driver keeps the previous behavior.
* Do not remove or rename parameters, and do not change their meaning.
* Do not change the format of volume or snapshot IDs.
  Kubernetes stores them in existing `PersistentVolume` and `VolumeSnapshotContent` resources.

## Changelog

`CHANGELOG.md` follows the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.
For every change that affects users of the driver, add an entry to the matching subsection, for example `Added`, `Changed`, or `Fixed`, under `[Unreleased]`.
Describe the effect on the user in one or two lines, not the implementation.
Changes to documentation, CI, or tests do not get an entry.

The release workflow uses the section of the released version as the body of the GitHub release.

## Commits

* Write the subject line in the imperative mood, for example `Add parameter to set the replica count`.
  Do not add a prefix such as `fix:`.
* Keep each commit to one logical change, including the files regenerated for it.
* Sign off every commit.

This repository uses the [Developer Certificate of Origin](https://developercertificate.org/).
Each commit must have a `Signed-off-by` line.
Add it by passing the `-s` flag to `git commit`:

```
$ git commit -s -m 'Add parameter to set the replica count'
```

### Commit hooks

The commit hooks check that new and changed files are formatted and that generated files are up to date.
To set up the commit hooks, install `pre-commit` and run:

```
$ pre-commit install
```

The hooks in `.pre-commit-config.yaml` run the following checks:

* Remove trailing whitespace, fix line endings at the end of files, check YAML syntax, and reject large files.
* Run `go fmt`, `goimports`, and `go mod tidy`.
* Run `go generate ./...` to regenerate the files listed in [Generated files](#generated-files).
* Run `make lint` on code changed since `HEAD`.

If a hook fails or modifies files, `pre-commit` aborts the commit.
Review the changes, add them, and commit again.

## Tests

### Unit tests

Unit tests use plain Go tests with `testify`.
Tests in `pkg/client` use the generated mocks in `pkg/client/mocks/` in place of a LINSTOR controller.
Run them with `go test`:

```
$ go test ./...
$ go test ./pkg/volume/ -run TestParseSnapshotId
```

### Sanity tests

The sanity tests run the driver against a real LINSTOR cluster.
The Makefile runs the privileged steps with `sudo`.
These steps create a loopback LVM thin pool and run the driver, which mounts volumes.

* `make sanity` runs the [CSI sanity suite](https://github.com/kubernetes-csi/csi-test) against the driver.
* `make sanity-cg` runs the consistency group tests in `test/consistency_group_test.go`.

Both targets start a LINSTOR controller and satellite in containers.
After the tests finish or fail, the targets remove the containers and the thin pool.

To run tests several times against the same cluster, start the cluster with `make sanity-cluster-up`.
Run the driver and the tests by hand, with the commands from the `sanity` or `sanity-cg` target in the `Makefile`.
Remove the cluster with `make sanity-cluster-down`.

The Makefile variables `CONTAINER_TOOL`, `SCRATCH_FILE`, and `CONTROLLER_TIMEOUT` change the container tool, the location of the loopback file, and how long to wait for the controller.
For example, to use Podman:

```
$ make sanity CONTAINER_TOOL=podman
```

### Testing on a cluster

To try a change on a Kubernetes cluster, build and push an image to your registry:

```
$ make upload REGISTRY=registry.example.com/myname TAGS=dev
```

The command builds and pushes the `piraeus-csi` and `piraeus-csi-nfs-server` images.
Replace the images in your deployment with them.
If you deploy with [Piraeus Operator](https://github.com/piraeusdatastore/piraeus-operator), change the images in the image configuration of the operator.

## Automated checks

On every pull request, the workflows in `.github/workflows` run the following checks:

* `golangci-lint` on new issues
* `pre-commit run` on all files
* `go test ./...`
* `make sanity` and `make sanity-cg`
* A build of the container images

## Pull request review

Keep the commit history clean during the review.
Fold follow-up fixes into the commits that introduced the problem instead of adding separate commits that address review comments.
For example, create a fixup commit, squash it into its target, and update the pull request:

```
$ git commit -s --fixup=<commit>
$ git rebase -i --autosquash <commit>~
$ git push --force-with-lease
```

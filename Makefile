REGISTRY ?= quay.io/piraeusdatastore
PLATFORMS ?= linux/amd64,linux/arm64
VERSION ?= $(shell git describe --tags --match "v*.*" HEAD)
TAGS ?= $(VERSION),latest
NOCACHE ?= false
ARGS ?=

CONTAINER_TOOL ?= docker
CONTROLLER_TIMEOUT ?= 30
SCRATCH_FILE = /mnt/linstor-scratch
SCRATCH_FILE_SIZE = 10G
# Privileged commands are prefixed with $(SUDO), everything else (notably go,
# which should populate the invoking user's build cache) runs unprivileged.
SUDO ?= sudo

help:
	@echo "Useful targets: 'print', 'update', 'upload'"

all: upload

.PHONY: print
print:
	$(MAKE) bake ARGS="$(ARGS) --print"

.PHONY: update
update:
	$(MAKE) bake ARGS="$(ARGS) --no-cache=$(NOCACHE) --pull=$(NOCACHE)"

.PHONY: upload
upload:
	$(MAKE) bake ARGS="$(ARGS) --no-cache=$(NOCACHE) --pull=$(NOCACHE) --push"

.PHONY: bake
bake:
	REGISTRY=$(REGISTRY) PLATFORMS=$(PLATFORMS) TAGS=$(TAGS) VERSION=$(VERSION) docker buildx bake $(ARGS)

.PHONY: linstor-csi
linstor-csi:
	go build ./cmd/linstor-csi

.PHONY: csi-sanity
csi-sanity:
	go build -o csi-sanity github.com/kubernetes-csi/csi-test/v5/cmd/csi-sanity

# CLUSTER_UP / CLUSTER_DOWN are the shared cluster fixture for the sanity suites: a loopback LVM-thin pool
# plus a real LINSTOR controller+satellite (piraeus-server containers) with a node and storage pool. They are
# kept as variables (not recursive make targets) so the suites can run them inline under a shell EXIT trap
# without dragging $(MAKE) into a recipe line — a line containing $(MAKE) would execute even under `make -n`.
#
# CLUSTER_UP is &&-chained: it stops at the first failure. CLUSTER_DOWN is ;-chained: every step runs even
# when the cluster only partially came up (a missing LV / VG / PV / loop device / container is fine, we just
# want it gone), making teardown safe to run after a partial or failed setup.
CLUSTER_UP = \
	$(SUDO) fallocate -l $(SCRATCH_FILE_SIZE) $(SCRATCH_FILE) && \
	$(SUDO) losetup -f $(SCRATCH_FILE) && \
	$(SUDO) pvcreate $$(losetup -O NAME -n -j $(SCRATCH_FILE)) && \
	$(SUDO) vgcreate scratch $$(losetup -O NAME -n -j $(SCRATCH_FILE)) && \
	$(SUDO) lvcreate --type thin-pool -l 100%FREE --name thin scratch && \
	$(SUDO) modprobe dm-snapshot && \
	$(CONTAINER_TOOL) run --rm -d --name controller --net host quay.io/piraeusdatastore/piraeus-server:latest startController && \
	$(CONTAINER_TOOL) run --rm -d --name satellite --privileged -v /dev:/dev --net host quay.io/piraeusdatastore/piraeus-server:latest startSatellite && \
	( echo "Waiting for LINSTOR Controller..."; for i in $$(seq 1 $(CONTROLLER_TIMEOUT)); do curl -fs -o /dev/null http://localhost:3370/health && exit 0; sleep 5; done; exit 1 ) && \
	$(CONTAINER_TOOL) exec controller linstor controller set-log-level TRACE && \
	$(CONTAINER_TOOL) exec controller linstor node create sanity-host 127.0.0.1 && \
	$(CONTAINER_TOOL) exec controller linstor storage-pool create lvmthin sanity-host thin-pool scratch/thin && \
	$(CONTAINER_TOOL) exec controller linstor node set-log-level sanity-host TRACE

CLUSTER_DOWN = \
	$(SUDO) lvremove -ff scratch/thin; \
	$(SUDO) vgremove -ff scratch; \
	$(SUDO) pvremove -ff $$(losetup -O NAME -n -j $(SCRATCH_FILE)); \
	$(SUDO) losetup -d $$(losetup -O NAME -n -j $(SCRATCH_FILE)); \
	$(CONTAINER_TOOL) stop controller satellite

# Standalone helpers for iterating against a long-lived cluster.
.PHONY: sanity-cluster-up
sanity-cluster-up:
	$(CLUSTER_UP)

.PHONY: sanity-cluster-down
sanity-cluster-down:
	$(CLUSTER_DOWN)

# The cluster fixture is torn down from an EXIT trap so it runs even when setup or the test fails partway,
# while the recipe still exits with the test's status. The driver performs real mounts and its socket is
# root-owned, so both the driver and csi-sanity run as root.
.PHONY: sanity
sanity: linstor-csi csi-sanity
	trap '$(CLUSTER_DOWN)' EXIT; \
	$(CLUSTER_UP) || exit $$?; \
	$(SUDO) ./linstor-csi -csi-endpoint unix:///tmp/csi.sock -node sanity-host & \
	$(SUDO) ./csi-sanity -csi.endpoint=unix:///tmp/csi.sock -csi.testvolumeparameters=test/testdata/params.yml; \
	exit $$?

# Consistency-group e2e suite (test/consistency_group_test.go). The suite skips unless CG_TEST_CONTROLLER
# points at a running controller, which this target provides; it reuses the same cluster fixture as `sanity`,
# but instead of csi-sanity it runs the in-process CG harness, which owns the *driver.Driver and a fake
# dynamic Kubernetes client. The test binary is built unprivileged (populating the invoking user's build
# cache) and run as root so TestCGNodePublish can do real attach/mounts; the other scenarios only need the
# LINSTOR cluster. Teardown runs from an EXIT trap, even on a partial start.
.PHONY: sanity-cg
sanity-cg:
	go test -c -o test/cg-sanity.test ./test/
	trap '$(CLUSTER_DOWN)' EXIT; \
	$(CLUSTER_UP) || exit $$?; \
	$(SUDO) CG_TEST_CONTROLLER=http://localhost:3370 ./test/cg-sanity.test -test.v -test.count=1; \
	exit $$?

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

.PHONY: sanity
sanity: linstor-csi csi-sanity
	$(SUDO) fallocate -l $(SCRATCH_FILE_SIZE) $(SCRATCH_FILE)
	$(SUDO) losetup -f $(SCRATCH_FILE)
	$(SUDO) pvcreate $$(losetup -O NAME -n -j $(SCRATCH_FILE))
	$(SUDO) vgcreate scratch $$(losetup -O NAME -n -j $(SCRATCH_FILE))
	$(SUDO) lvcreate --type thin-pool -l 100%FREE --name thin scratch
# Load dm-snapshot for LINSTOR snapshot support: the satellite cannot
# modprobe host modules from inside its container.
	$(SUDO) modprobe dm-snapshot
	$(CONTAINER_TOOL) run --rm -d --name controller --net host quay.io/piraeusdatastore/piraeus-server:latest startController
	$(CONTAINER_TOOL) run --rm -d --name satellite --privileged -v /dev:/dev --net host quay.io/piraeusdatastore/piraeus-server:latest startSatellite
	@echo "Waiting for LINSTOR Controller..."
	@for i in $$(seq 1 $(CONTROLLER_TIMEOUT)); do if curl -fs -o /dev/null http://localhost:3370/health; then exit 0; fi; sleep 5; done; exit 1
	$(CONTAINER_TOOL) exec controller linstor node create sanity-host 127.0.0.1
	$(CONTAINER_TOOL) exec controller linstor storage-pool create lvmthin sanity-host thin-pool scratch/thin
# The driver performs real mounts, and connecting to its root-owned socket
# requires write permission, so both the driver and csi-sanity run as root.
	$(SUDO) ./linstor-csi -csi-endpoint unix:///tmp/csi.sock -node sanity-host &
	$(SUDO) ./csi-sanity -csi.endpoint=unix:///tmp/csi.sock -csi.testvolumeparameters=test/testdata/params.yml
	$(SUDO) lvremove -ff scratch/thin
	$(SUDO) vgremove -ff scratch
	$(SUDO) pvremove -ff $$(losetup -O NAME -n -j $(SCRATCH_FILE))
	$(SUDO) losetup -d $$(losetup -O NAME -n -j $(SCRATCH_FILE))
	$(CONTAINER_TOOL) stop controller satellite

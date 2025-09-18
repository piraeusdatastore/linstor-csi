REGISTRY ?= quay.io/piraeusdatastore
PLATFORMS ?= linux/amd64,linux/arm64
VERSION ?= $(shell git describe --tags --match "v*.*" HEAD)
TAGS ?= $(VERSION),latest
NOCACHE ?= false
ARGS ?=

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

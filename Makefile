PROJECT ?= piraeus-csi
REGISTRY ?= quay.io/piraeusdatastore
PLATFORMS ?= linux/amd64,linux/arm64
VERSION ?= $(shell git describe --tags --match "v*.*" HEAD)
TAG ?= $(VERSION)
NOCACHE ?= false

help:
	@echo "Useful targets: 'update', 'upload'"

all: update upload

.PHONY: update
update:
	for r in $(REGISTRY); do \
		docker buildx build $(_EXTRA_ARGS) \
			--build-arg=VERSION=$(VERSION) \
			--platform=$(PLATFORMS) \
			--no-cache=$(NOCACHE) \
			--pull=$(NOCACHE) \
			--tag $$r/$(PROJECT):$(TAG) \
			--tag $$r/$(PROJECT):latest \
			. ;\
	done

.PHONY: upload
upload:
	make update _EXTRA_ARGS=--push

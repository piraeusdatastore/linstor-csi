PROJECT ?= linstor-csi
REGISTRY ?= piraeusdatastore
TAG ?= latest
NOCACHE ?= false

GO ?= go
OS ?= linux
ARCH ?= amd64
VERSION=$(shell git describe --tags --always --dirty)
LDFLAGS = -X github.com/piraeusdatastore/linstor-csi/pkg/driver.Version=${VERSION}

help:
	@echo "Useful targets: 'update', 'upload'"

all: update upload

.PHONY: update
update:
	docker build --no-cache=$(NOCACHE) -t $(PROJECT):$(TAG) .
	docker tag $(PROJECT):$(TAG) $(PROJECT):latest

.PHONY: upload
upload:
	for r in $(REGISTRY); do \
		docker tag $(PROJECT):$(TAG) $$r/$(PROJECT):$(TAG) ; \
		docker tag $(PROJECT):$(TAG) $$r/$(PROJECT):latest ; \
		docker push $$r/$(PROJECT):$(TAG) ; \
		docker push $$r/$(PROJECT):latest ; \
	done

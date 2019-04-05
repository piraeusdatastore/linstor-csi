# CSI Driver for Linstor
# Copyright © 2018 LINBIT USA, LLC
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>.
#

OS=linux
ARCH=amd64

GO = go
PROJECT_NAME = linstor-csi
VERSION=$(shell git describe --tags --always --dirty)
LATESTTAG=$(shell git describe --abbrev=0 --tags | tr -d 'v')
LDFLAGS = -X github.com/LINBIT/linstor-csi/pkg/driver.Version=${VERSION}
DOCKERREGISTRY = drbd.io
DOCKERREGISTRY_QUAY = quay.io
DOCKERREGPATH = $(DOCKERREGISTRY)/$(PROJECT_NAME)
DOCKERREGPATH_QUAY = $(DOCKERREGISTRY_QUAY)/linbit/$(PROJECT_NAME)
DOCKERREGPATH_DOCKER = linbit/$(PROJECT_NAME)
DOCKER_TAG ?= latest

RM = rm
RM_FLAGS = -vf

all: build

glide:
	glide update  --strip-vendor
	glide-vc --only-code --no-tests --use-lock-file

build:
	go build -ldflags '$(LDFLAGS)' cmd/$(PROJECT_NAME)/$(PROJECT_NAME).go

release:
	GOOS=$(OS) GOARCH=$(ARCH) $(GO) build -ldflags '$(LDFLAGS)' -o $(PROJECT_NAME)-$(OS)-$(ARCH)

staticrelease:
	GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0  $(GO) build -a -ldflags '$(LDFLAGS) -extldflags "-static"' -o $(PROJECT_NAME)-$(OS)-$(ARCH) cmd/$(PROJECT_NAME)/$(PROJECT_NAME).go

dockerimage: distclean
	docker build -t $(DOCKERREGPATH):$(DOCKER_TAG) .
	docker tag $(DOCKERREGPATH):$(DOCKER_TAG) $(DOCKERREGPATH):latest
	docker tag $(DOCKERREGPATH):$(DOCKER_TAG) $(DOCKERREGPATH_QUAY):$(DOCKER_TAG)
	docker tag $(DOCKERREGPATH_QUAY):$(DOCKER_TAG) $(DOCKERREGPATH_QUAY):latest
	docker tag $(DOCKERREGPATH):$(DOCKER_TAG) $(DOCKERREGPATH_DOCKER):$(DOCKER_TAG)
	docker tag $(DOCKERREGPATH_DOCKER):$(DOCKER_TAG) $(DOCKERREGPATH_DOCKER):latest

.PHONY: dockerpath
dockerpath:
	@echo $(DOCKERREGPATH):$(DOCKER_TAG) $(DOCKERREGPATH):latest \
		$(DOCKERREGPATH_QUAY):$(DOCKER_TAG) $(DOCKERREGPATH_QUAY):latest \
		$(DOCKERREGPATH_DOCKER):$(DOCKER_TAG) $(DOCKERREGPATH_DOCKER):latest

clean:
	$(RM) $(RM_FLAGS) $(PROJECT_NAME)-$(OS)-$(ARCH)

distclean: clean

# packaging, you need the packaging branch for these
#
# we build binary-only packages and use the static binary in this tarball
$(PROJECT_NAME)-$(LATESTTAG).tar.gz: staticrelease
	dh_clean || true
	mv $(PROJECT_NAME)-$(OS)-$(ARCH) $(PROJECT_NAME)
	tar --transform="s,^,$(PROJECT_NAME)-$(LATESTTAG)/," --owner=0 --group=0 -czf $@ \
		$(PROJECT_NAME) Makefile Dockerfile

# consistency with the other linbit projects
debrelease: $(PROJECT_NAME)-$(LATESTTAG).tar.gz

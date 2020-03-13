PROJECT ?= linstor-csi

GO ?= go
OS ?= linux
ARCH ?= amd64
VERSION = $(shell git describe --tags --always --dirty)
LDFLAGS = -X github.com/piraeusdatastore/linstor-csi/pkg/driver.Version=${VERSION}

staticrelease:
	GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0  $(GO) build -a -ldflags '$(LDFLAGS) -extldflags "-static"' -o $(PROJECT)-$(OS)-$(ARCH) cmd/$(PROJECT)/$(PROJECT).go

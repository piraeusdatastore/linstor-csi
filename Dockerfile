FROM --platform=$BUILDPLATFORM golang:1 AS builder

WORKDIR /src
COPY --link go.mod go.sum /src/
RUN go mod download

COPY --link . /src/

ARG TARGETARCH
ARG TARGETOS
ARG VERSION=unknown
RUN --mount=type=cache,target=/root/.cache/go-build GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build \
    -a \
    -ldflags "-X github.com/piraeusdatastore/linstor-csi/pkg/driver.Version=$VERSION -extldflags -static"  \
    -o /linstor-csi  \
    ./cmd/linstor-csi/linstor-csi.go

FROM --platform=$BUILDPLATFORM golang:1 AS downloader

ARG TARGETOS
ARG TARGETARCH
ARG LINSTOR_WAIT_UNTIL_VERSION=v0.3.1
RUN curl -fsSL https://github.com/LINBIT/linstor-wait-until/releases/download/$LINSTOR_WAIT_UNTIL_VERSION/linstor-wait-until-$LINSTOR_WAIT_UNTIL_VERSION-$TARGETOS-$TARGETARCH.tar.gz | tar xvzC /

FROM debian:trixie-slim
ARG LINSTOR_WAIT_UNTIL

RUN apt-get update && apt-get install -y --no-install-recommends \
      xfsprogs e2fsprogs nfs-common \
      && apt-get clean && rm -rf /var/lib/apt/lists/* \
      && ln -sf /proc/mounts /etc/mtab

COPY --from=builder /linstor-csi /
COPY --from=downloader /linstor-wait-until /linstor-wait-until

ENTRYPOINT ["/linstor-csi"]

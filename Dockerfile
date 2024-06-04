FROM --platform=$BUILDPLATFORM golang:1 as builder
MAINTAINER Roland Kammerer <roland.kammerer@linbit.com>

WORKDIR /src
COPY go.mod go.sum /src/
RUN go mod download

COPY . /src/
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=unknown
RUN --mount=type=cache,target=/root/.cache/go-build GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build \
    -a \
    -ldflags "-X github.com/piraeusdatastore/linstor-csi/pkg/driver.Version=$VERSION -extldflags -static"  \
    -o /linstor-csi  \
    ./cmd/linstor-csi/linstor-csi.go

FROM --platform=$BUILDPLATFORM golang:1 as downloader

ARG TARGETOS
ARG TARGETARCH
ARG LINSTOR_WAIT_UNTIL_VERSION=v0.2.2
RUN curl -fsSL https://github.com/LINBIT/linstor-wait-until/releases/download/$LINSTOR_WAIT_UNTIL_VERSION/linstor-wait-until-$LINSTOR_WAIT_UNTIL_VERSION-$TARGETOS-$TARGETARCH.tar.gz | tar xvzC /

FROM debian:bookworm-slim
ARG LINSTOR_WAIT_UNTIL

RUN apt-get update && apt-get install -y --no-install-recommends \
      xfsprogs e2fsprogs \
      && apt-get clean && rm -rf /var/lib/apt/lists/* \
      && ln -sf /proc/mounts /etc/mtab

COPY --from=builder /linstor-csi /
COPY --from=downloader /linstor-wait-until /linstor-wait-until

ENTRYPOINT ["/linstor-csi"]

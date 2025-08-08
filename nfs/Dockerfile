FROM --platform=$BUILDPLATFORM golang:1 AS builder

WORKDIR /src
COPY --link go.mod go.sum /src/
RUN go mod download
COPY --link ./cmd /src/cmd
COPY --link ./pkg /src/pkg

ARG TARGETARCH
ARG TARGETOS
ARG VERSION=unknown
RUN --mount=type=cache,target=/root/.cache/go-build GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build \
    -a \
    -ldflags "-X github.com/piraeusdatastore/linstor-csi/pkg/driver.Version=$VERSION -extldflags -static"  \
    -o /nfs-helper  \
    ./cmd/nfs-helper

FROM debian:bookworm

RUN { echo 'APT::Install-Recommends "false";' ; echo 'APT::Install-Suggests "false";' ; } > /etc/apt/apt.conf.d/99_piraeus

RUN --mount=type=cache,target=/var/cache,sharing=private \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=private \
    --mount=type=tmpfs,target=/var/log \
    apt-get update && \
    apt-get install -y wget ca-certificates && \
    wget https://packages.linbit.com/public/linbit-keyring.deb -O /var/cache/linbit-keyring.deb && \
    dpkg -i /var/cache/linbit-keyring.deb && \
    . /etc/os-release && \
    echo "deb [signed-by=/etc/apt/trusted.gpg.d/linbit-keyring.gpg] http://packages.linbit.com/public $VERSION_CODENAME misc" > /etc/apt/sources.list.d/linbit.list

RUN --mount=type=cache,target=/var/cache,sharing=private \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=private \
    --mount=type=tmpfs,target=/var/log \
    apt-get update && \
    apt-get install -y --no-install-recommends drbd-reactor resource-agents nfs-ganesha nfs-ganesha-vfs systemd kmod psmisc procps && \
    cp /usr/share/doc/drbd-reactor/examples/drbd-reactor-reload.path /usr/share/doc/drbd-reactor/examples/drbd-reactor-reload.service /etc/systemd/system/ && \
    sed -e 's/usage-count yes;/usage-count no;/' -i /etc/drbd.d/global_common.conf

COPY --from=builder --link /nfs-helper /usr/bin/nfs-helper
COPY --link nfs/service/nfs-ganesha@.service nfs/service/systemd-growfs@.service nfs/service/prepare-device-links@.service nfs/service/advertise-nfs-endpoint@.service nfs/service/clean-nfs-endpoint@.service nfs/service/start-stop-reactor.service /usr/lib/systemd/system/
COPY --link nfs/service/drbd@.service.override.conf /usr/lib/systemd/system/drbd@.service.d/container.conf
COPY --link nfs/service/drbd-demote-or-escalate@.service.override.conf /usr/lib/systemd/system/drbd-demote-or-escalate@.service.d/container.conf
COPY --link nfs/service/default-config.tmpl /etc/nfs-helper/default-config.tmpl

RUN \
    # Random stuff we don't need in the container.
    systemctl disable nfs-client.target nfs-ganesha.service logd.service apt-daily-upgrade.timer apt-daily.timer dpkg-db-backup.timer e2scrub_all.timer && \
    systemctl mask systemd-tmpfiles-setup.service systemd-tmpfiles-clean.timer systemd-binfmt.service && \
    # The reason this image exists. reactor is started by the reconcile-nfs service. \
    systemctl disable drbd-reactor.service && \
    systemctl enable start-stop-reactor.service

STOPSIGNAL SIGRTMIN+3
ENV container=oci SYSTEMD_GETTY_AUTO=no
CMD ["/bin/systemd"]

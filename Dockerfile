FROM golang:1 as builder
MAINTAINER Roland Kammerer <roland.kammerer@linbit.com>

ARG TARGETARCH

COPY . /usr/local/go/linstor-csi/
RUN cd /usr/local/go/linstor-csi && make ARCH=${TARGETARCH} -f container.mk staticrelease && mv ./linstor-csi-linux-${TARGETARCH} /linstor-csi

FROM debian:buster
RUN apt-get update && apt-get install -y --no-install-recommends \
      xfsprogs e2fsprogs \
      && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /linstor-csi /
ENTRYPOINT ["/linstor-csi"]

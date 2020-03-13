FROM golang:1 as builder

COPY . /usr/local/go/linstor-csi/
RUN cd /usr/local/go/linstor-csi && make staticrelease && mv ./linstor-csi-linux-amd64 /
FROM debian:buster
RUN apt-get update && apt-get install -y --no-install-recommends \
      xfsprogs e2fsprogs \
      && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /linstor-csi-linux-amd64 /linstor-csi
ENTRYPOINT ["/linstor-csi"]

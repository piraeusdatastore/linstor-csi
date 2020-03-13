FROM golang:1.13.8 as builder
COPY . /usr/local/go/linstor-csi/
# keep on single line:
RUN cd /usr/local/go/linstor-csi && make staticrelease && mv ./linstor-csi-linux-amd64 / # !lbbuild
# =lbbuild RUN cp /usr/local/go/linstor-csi/linstor-csi /linstor-csi-linux-amd64
FROM debian:buster
RUN apt-get update && apt-get install -y --no-install-recommends \
      xfsprogs=4.20.0-1 \
      && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /linstor-csi-linux-amd64 /linstor-csi
ENTRYPOINT ["/linstor-csi"]

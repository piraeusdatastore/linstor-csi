FROM golang:latest as builder
ADD . /usr/local/go/src/github.com/LINBIT/linstor-csi/
# keep on single line:
RUN cd /usr/local/go/src/github.com/LINBIT/linstor-csi && make staticrelease && mv ./linstor-csi-linux-amd64 / # !lbbuild
# =lbbuild RUN cp /usr/local/go/src/github.com/LINBIT/linstor-csi/linstor-csi /linstor-csi-linux-amd64
FROM alpine
RUN apk update && apk upgrade && apk add xfsprogs e2fsprogs
MAINTAINER Hayley Swimelar <hayley@linbit.com>
COPY --from=builder /linstor-csi-linux-amd64 /linstor-csi
ENTRYPOINT ["/linstor-csi"]

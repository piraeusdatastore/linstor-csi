FROM golang:1.13.8 as builder
COPY . /usr/local/go/linstor-csi/
# keep on single line:
RUN cd /usr/local/go/linstor-csi && make staticrelease && mv ./linstor-csi-linux-amd64 / # !lbbuild
# =lbbuild RUN cp /usr/local/go/linstor-csi/linstor-csi /linstor-csi-linux-amd64

FROM centos:centos8 as cent8
# nothing, just get it for the repos (i.e. FS-progs)

FROM registry.access.redhat.com/ubi8/ubi
RUN yum -y update-minimal --security --sec-severity=Important --sec-severity=Critical && \
	yum clean all -y

COPY --from=cent8 /etc/pki/rpm-gpg/RPM-GPG-KEY-centosofficial /etc/pki/rpm-gpg/
COPY --from=cent8 /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/
RUN yum install -y --disablerepo="*" --enablerepo=BaseOS e2fsprogs xfsprogs && \
	rm -f /etc/yum.repos.d/CentOS-Base.repo && yum clean all -y

ENV CSI_VERSION 0.7.4

ARG release=1
LABEL name="LINSTOR CSI driver" \
      vendor="LINBIT" \
      version="$CSI_VERSION" \
      release="$release" \
      summary="LINSTOR's CSI driver component" \
      description="LINSTOR's CSI driver component"
COPY LICENSE /licenses/gpl-2.0.txt

COPY --from=builder /linstor-csi-linux-amd64 /linstor-csi
ENTRYPOINT ["/linstor-csi"]

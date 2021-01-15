# Linstor CSI Plugin

![build](https://github.com/piraeusdatastore/linstor-csi/workflows/build/badge.svg)
![tests](https://github.com/piraeusdatastore/linstor-csi/workflows/tests/badge.svg)
![latest version](https://img.shields.io/github/v/tag/piraeusdatastore/linstor-csi?label=version&sort=semver)

This CSI plugin allows for the use of LINSTOR volumes on Container Orchestrators
that implement CSI, such as Kubernetes.

# Building

If you wish to create a docker image for a local registry
run `make staticrelease`.

# Deployment

If you are looking to deploy a full LINSTOR setup with LINSTOR controller and satellites,
take a look at [our operator](https://github.com/piraeusdatastore/piraeus-operator).

This project _ONLY_ deploys the CSI components, a working LINSTOR cluster is required.

## Kubernetes

The yaml file in `examples/k8s/deploy` shows an example configuration which
will deploy the LINSTOR csi plugin along with the needed k8s sidecar containers.
You will need to change all instances of `LINSTOR_IP` to point to the controller(s)
of the LINSTOR cluster that you wish this plugin to interact with.

You will need to enable the following feature gates on both the kube-apiserver
and all kubelets for this plugin to be operational: `CSINodeInfo=true`,
`CSIDriverRegistry=true,VolumeSnapshotDataSource=true`. Please ensure that your
version of Kubernetes is recent enough to enable these gates.

# Usage

This project must be used in conjunction with a working LINSTOR cluster, version
0.9.11 or better.
[LINSTOR's documentation](https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/)
is the foremost guide on setting up and administering LINSTOR.

## :warning:️ Known issues

* Due to the way [ZFS snapshots work], provisioning new Volumes from existing Volumes
  does not work using ZFS storage pools. The internal temporary snapshot cannot be
  deleted after the new volume is created.

  As a workaround, first create a VolumeSnapshot of the existing volume and restore from
  that snapshot. Also read the issue below!

* Deletion of VolumeSnapshots will fail for ZFS based volumes if a volume restored from
  the snapshot exists in the cluster.

[ZFS snapshots work]: https://docs.oracle.com/cd/E23824_01/html/821-1448/gbciq.html

## Kubernetes

After the plugin has been deployed, you're free to create storage classes
that point to the name of the external provisioner associateed with the CSI plugin
and have your users start provisioning volumes from them. A basic storage class could
look like this:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: linstor-basic-storage
provisioner: linstor.csi.linbit.com
allowVolumeExpansion: true
parameters:
  placementCount: "2"
  storagePool: "my-storage-pool"
  resourceGroup: "linstor-basic-storage"
  csi.storage.k8s.io/fstype: xfs
```

A full list of all parameters usable in a storage class is available
[here](https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#s-kubernetes-sc-parameters).

Ensure that all kubelets that are expected to use LINSTOR volumes have a running
LINSTOR satellite that is configured to work with the LINSTOR controller
configured in the plugin's deployment files and that the storage pool indicated
in the storage class has been properly configured. This pool does not need to be
present on the Kubelets themselves for volumes attached over the network.

Most of the documentation for using this project with Kubernetes is located
[here](https://docs.linbit.com/docs/users-guide-9.0/#ch-kubernetes).

## Kubevirt

An example of using the CSI driver in combination with kubevirt (block device mode, live migration) can be
found in the `examples/kubevirt/` directory.

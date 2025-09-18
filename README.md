# Linstor CSI Plugin

[![build](https://github.com/piraeusdatastore/linstor-csi/actions/workflows/build.yml/badge.svg)](https://github.com/piraeusdatastore/linstor-csi/actions/workflows/build.yml)
[![tests](https://github.com/piraeusdatastore/linstor-csi/actions/workflows/tests.yml/badge.svg)](https://github.com/piraeusdatastore/linstor-csi/actions/workflows/tests.yml)
![latest version](https://img.shields.io/github/v/tag/piraeusdatastore/linstor-csi?label=version&sort=semver)

This CSI plugin allows for the use of LINSTOR volumes on Container Orchestrators
that implement CSI, such as Kubernetes.

# Building

If you wish to create a docker image for a local registry run `make upload REGISTRY=local.example.com`.

# Deployment

If you are looking to deploy a full LINSTOR setup with LINSTOR controller and satellites,
take a look at [our operator](https://github.com/piraeusdatastore/piraeus-operator).

This project _ONLY_ deploys the CSI components, a working LINSTOR cluster is required.
Use our example deployment in `examples/k8s/deploy` as base to deploy only the LINSTOR CSI
components. You will need to update every occurence of `LINSTOR_CONTROLLER_URL` with the actual
URL of your LINSTOR Controller, for example like this:

```
$ LINSTOR_CONTROLLER_URL=http://linstor-controller.example.com:3370
$ kubectl kustomize http://github.com/piraeusdatastore/linstor-csi/examples/k8s/deploy \
  | sed "s#LINSTOR_CONTROLLER_URL#$LINSTOR_CONTROLLER_URL#" \
  | kubectl apply --server-side -f -
```

# Usage

This project must be used in conjunction with a working LINSTOR cluster, version
0.9.11 or better.
[LINSTOR's documentation](https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/)
is the foremost guide on setting up and administering LINSTOR.

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
  linstor.csi.linbit.com/placementCount: "2"
  linstor.csi.linbit.com/storagePool: "my-storage-pool"
  linstor.csi.linbit.com/resourceGroup: "linstor-basic-storage"
  csi.storage.k8s.io/fstype: xfs
  # You can override LINSTOR properties by adding the property.linstor.csi.linbit.com prefix:
  property.linstor.csi.linbit.com/DrbdOptions/auto-quorum: suspend-io
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

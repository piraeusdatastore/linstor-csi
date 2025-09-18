# Design: ReadWriteMany support

ReadWriteMany (RWX) support, allowing concurrent reads and writes to the same volume from multiple nodes, is a much
requested feature.

## Existing solutions

Over the years there have been projects that enabled RWX on top of existing storage solutions.

* [nfs-ganesha-server-and-external-provisioner](https://github.com/kubernetes-sigs/nfs-ganesha-server-and-external-provisioner/)
  This is a project by the Kubernetes SIG Storage. It creates a single NFS server instance backed by a single
  PersistentVolume. This PersistentVolume is then used to dynamically allocate NFS Volumes based on subdirectory
  exports.
* [External-NFS-Server-Provisioner](https://github.com/PHOENIX-MEDIA/External-NFS-Server-Provisioner)
  This project is known to work with specifically Piraeus. It also works by exporting a specific PV as NFS, but with an
  added High Availability component.

Both of these solutions are limited in that they can only use a single existing PV as backing volume, making creation
of new volumes more involved than expected from a fully automatic solution.

Another limitation is that the PV acts as a single point of failure in the system: should the volume be unavailable, all
NFS volumes are also affected.

## Acceptance criteria for solution

The following criteria are the required baseline of features we want for our RWX implementation.

1. Dynamic provisioning of new volumes.
2. Resizing of volumes.

The existing solutions both already fail at the 1. requirement: nfs-ganesha supports dynamic provisioning, but only
limited up to the size of the "source" volume, which is not considered acceptable. The "External-NFS" solution does
not support dynamic provisioning at all.

The following criteria are additional features we want to support, if possible.

1. Retaining existing StorageClass configuration for LINSTOR CSI.
2. Retaining the mapping of one LINSTOR Resource Definition per provisioned volume.
3. "Fail over" in case of node failures.
4. Use as little cluster resource as possible

# Solution

## Design Choices

### Choice of technology: NFS

Early on, other possible technologies to support RWX volumes were considered. These were all eventually dismissed for
the following reasons:

* 9p filesystem: not widely available in common Linux distributions.
* SMB/CIFS: not enough experience by the author.
* Any kind of "Cluster aware filesystem": no official supported with DRBD as device sharing medium.

NFS Clients are available in basically all in-use Linux kernels, simplifying the use of NFS as technology.


### Choice of NFS provider: NFS Ganesha

Initially, nfsd, the Linux kernel NFS server was considered. However, it turns out that this would make node failure
difficult to handle:

To allow lock recovery after fail over, nfsd uses a database maintained by a user space component. Per node, there can
only be one such database. It is not possible to maintain this database "per export", so that individual volumes can
fail over to different nodes.

This left user-space NFS server [NFS Ganesha](https://nfs-ganesha.org/), which can be started on a per-export basis,
maintaining its own client tracking database per export.

### Choice of NFS protocol: v4

NFSv3 requires additional components listening on multiple ports  while NFSv4 requires a single port without any
additional helper daemons for client tracking or mounting. All relevant kernels support NFSv4, making this choice easy.

## Changes to LINSTOR CSI

In order to retain the existing StorageClass parameters and mapping of volumes to resource definitions, we will re-use
the existing workflow with the following additions:

* If RWX is requested, `CreateVolume()` in addition to the regular "VolNr 0" will add a "VolNr 1" to the created RD.
  This is for storing the per-volume client tracking information of the NFS server to enable smooth fail over without
  lost locks.
* `CreateVolume()` will also create the necessary configuration for NFS server, in particular containing name of the RD
  and the "URL" of the NFS export.
* The `ControllerPublish()`/`ControllerUnpublish()` calls are skipped for RWX volumes, as we do not need a local device
  for NFS exports.
* `NodePublish()` will use `mount -t nfs` to mount the volume, based on the configured URL from `CreateVolume()`
* `NodePublish()` and `NodeExpand()` will not try to expand NFS volumes, as that needs to happen on the server side.

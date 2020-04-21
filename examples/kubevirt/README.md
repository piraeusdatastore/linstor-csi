# Using DRBD block devices for kubevirt

This demonstrates how to use DRBD block devices provisioned by the LINSTOR CSI driver. We assume that the
CSI driver is working (e.g., by installing it via it operator/helm chart), and that kubevirt also works and
that live migration is enabled and tested. Live migration is not a requirement, but it a lot more fun.

As my environment was pretty constrained and I did not care to use kubevirt's
[CDI](https://kubevirt.io/2018/CDI-DataVolumes.html), I prepared an Alpine image that I used. This also
allowed me to skip all the additional devices and configuration via cloud-init. This way I even did not have
to care about network setup. After all this is a demonstration on how to use the CSI driver in block mode for
live migration and not a kubevirt tutorial.

## Preparing a VM image
```bash
$ qemu-img create -f qcow2 alpine.qcow2 950M
$ qemu-system-x86_64 -m 512 -nic user -boot d -cdrom alpine-virt-3.11.5-x86_64.iso -enable-kvm -hda alpine.qcow2
# setup-alpine; no ssh; reboot; disable the NIC/eth0 in /etc/network/interfaces
$ qemu-img convert -O raw alpine.qcow2 -f qcow2 alpine.raw
$ xz alpine.raw
# scp alpine.raw.xz to one of the storage nodes/hypervisors
```

## Setting up the VM
First we have to set up a storage class that we want to use for our virtual machines. The key here for live
migration is to set the according DRBD parameter that allows two primaries. For a short moment during live
migration both VMs will have the block device opened read-write. That is perfectly fine as long as not both
write to it and is what other storage plugins do.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: linstor-kubevirt-storage
provisioner: linstor.csi.linbit.com
parameters:
  placementCount: "2"
  storagePool: "lvm-thin"
  resourceGroup: "kubevirt"
  DrbdOptions/Net/allow-two-primaries: "yes"
```

Running `kubectl create -f class.yaml` will create the storage class.

The next step is to actually create a PVC. If live migration should be used, make sure to set the "accessMode"
to "ReadWriteMany".

```yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: "alpine"
  annotations:
    volume.beta.kubernetes.io/storage-class: linstor-kubevirt-storage
spec:
  accessModes:
    - ReadWriteMany
  volumeMode: Block
  resources:
    requests:
      storage: 1Gi
```

Create the PVC with `kubectl create -f pvc-alpine.yaml`. This should create the actual DRBD resource (e.g.,
`/dev/drbd1000`) on two nodes. On one of them we now copy our raw image to the actual block device:

```bash
$ xzcat alpine.raw.xz | dd of=/dev/drbd1000 iflag=fullblock oflag=direct bs=4M
```

After that we can define our VM:
```yaml
apiVersion: kubevirt.io/v1alpha3
kind: VirtualMachine
metadata:
  creationTimestamp: 2018-07-04T15:03:08Z
  generation: 1
  labels:
    kubevirt.io/os: linux
  name: vm1
spec:
  running: true
  template:
    metadata:
      creationTimestamp: null
      labels:
        kubevirt.io/domain: vm1
    spec:
      domain:
        cpu:
          cores: 2
        devices:
          autoattachPodInterface: false
          disks:
          - disk:
              bus: virtio
            name: disk0
        machine:
          type: q35
        resources:
          requests:
            memory: 512M
      volumes:
      - name: disk0
        persistentVolumeClaim:
          claimName: alpine
```

We create the VM with `kubectl create -f vm1-alpine.yaml`. We can then connect to the VM with `./virtctl
console vm1`.

## Live migration
In order to watch live migration one can run `watch drbdsetup status` on one of the nodes. You should see that
the DRBD resource is single Primary on one node. Then issue `./virtctl migrate vm1`. After a short time you
will see that the DRBD resource is in Pirmary on both nodes and then single Primary on the one it was not
before. Then you can connect to the console (`./virtctl console vm1`) again and find your running VM as you
left it.

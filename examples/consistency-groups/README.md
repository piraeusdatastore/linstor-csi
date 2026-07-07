# Consistency groups

A *consistency group* places several PersistentVolumes as separate volumes of **one** LINSTOR/DRBD resource,
so that a snapshot taken across the group is crash-consistent — every volume captured at the same point in
time, exactly as a power loss would leave them. The motivating case is a KubeVirt virtual machine whose disks
are separate PVCs: a backup of the VM must capture all disks together.

## How membership works

CSI has no "create these volumes as one group" call — `CreateVolume` is invoked once per PVC with no knowledge
of its siblings. The group identity therefore travels on the **PVC itself**, as a label:

```yaml
metadata:
  labels:
    linstor.csi.linbit.com/consistency-group: fedora-vm-1
```

Every PVC carrying the same label value **in the same namespace** becomes a volume number in one shared
LINSTOR resource named `cg-<uuid>`. The group key is `(namespace, value)`, so identically named groups in
different namespaces never collide.

This example provisions **two** VMs — `fedora-vm-1` and `fedora-vm-2` — each its own consistency group, both
booted from a single **golden image** that is pulled from the registry only once.

## Prerequisites

1. **Enable the feature on the controller.** Add the flag to the `linstor-csi` container of the CSI controller
   Deployment:

   ```
   --enable-consistency-groups
   ```

2. **Pass PVC metadata to the driver.** The driver learns the PVC name/namespace from the external-provisioner,
   which must run with (this is the default in recent piraeus-operator deployments):

   ```
   --extra-create-metadata=true
   ```

3. **Grant RBAC.** The controller reads the label off the PVC, so its ServiceAccount needs `get` on
   `persistentvolumeclaims` — apply [`rbac.yaml`](./rbac.yaml) (adjust the ServiceAccount name/namespace to
   your deployment).

4. **Group snapshots** require the external-snapshotter with VolumeGroupSnapshot support
   (csi-snapshotter v8+ and the `groupsnapshot.storage.k8s.io` CRDs installed).

5. **The root disks** are provisioned by CDI DataVolumes, so [CDI (Containerized Data
   Importer)](https://github.com/kubevirt/containerized-data-importer) must be installed. CDI propagates a
   DataVolume's `consistency-group` label onto the PVC it creates, so the disk joins the group. A blank data
   disk needs no CDI.

6. **KubeVirt** (with `virtctl`) to run the VMs and open their consoles.

Consistency groups require the driver to run in Kubernetes (to read PVC labels). Volumes created without
the label are unaffected and keep the usual one-resource-per-volume layout.

## Golden image, without breaking consistency groups

Pulling the OS image from the registry for every VM is slow. The usual fix is a **golden image**: import the
image once, then *clone* it for each VM's root disk. But there is a catch specific to consistency groups —
the driver **rejects a content source on a group member** (LINSTOR clone/restore is whole-resource, so a
single cloned member cannot be slotted into a shared resource). A normal CDI smart-clone (snapshot or CSI
clone) sets exactly such a content source and would be refused.

The way around it is CDI's **host-assisted clone** (`cloneStrategy: copy`): CDI provisions the target root PVC
as a *fresh* volume — no content source, so it joins the group — and a clone pod copies the golden data in
afterwards. The registry image is still pulled only once (into the golden), and each VM's root disk is a local
block copy rather than a fresh registry pull. The `cdi.kubevirt.io/clone-strategy: copy` annotation on
[`storageclass.yaml`](./storageclass.yaml) selects this strategy (CDI feeds it into the StorageClass's
StorageProfile).

The only content source a group member *does* accept is a **group snapshot restore** — that is what
[`restore-vm.yaml`](./restore-vm.yaml) uses.

## Walkthrough

```sh
# 1. RBAC + StorageClass (all disks of one VM must use the same StorageClass — see caveats). The StorageClass
# carries the cdi.kubevirt.io/clone-strategy: copy annotation, so CDI host-assisted-clones the golden and the
# cloned root disks can join a consistency group.
kubectl apply -f rbac.yaml
kubectl apply -f storageclass.yaml

# 2. Import the golden image ONCE. Every VM's root disk is a clone of this PVC.
kubectl apply -f golden-image.yaml
kubectl get datavolume fedora-golden -w        # wait for phase: Succeeded

# 3. VM 1's disks: root (a host-assisted clone of the golden) + a blank data disk. Both carry the
# consistency-group label 'fedora-vm-1', so they land in one LINSTOR resource.
kubectl apply -f vm1-disks.yaml

# Confirm both disks resolved to the same cg- resource at different volume numbers:
#   linstor volume-definition list
# You should see one cg-<uuid> resource with two volume definitions (0 and 1).

# 4. VM 1 itself. Log in on the console as fedora / password:
kubectl apply -f vm1.yaml
#   virtctl console fedora-vm-1

# 5. VM 2 — an independent second VM from the SAME golden (its own group 'fedora-vm-2'), for comparison.
kubectl apply -f vm2-disks.yaml
kubectl apply -f vm2.yaml

# 6. Snapshot VM 1's whole group, crash-consistent.
kubectl apply -f volume-group-snapshot-class.yaml
kubectl apply -f volume-group-snapshot.yaml

# 7. Restore VM 1's group into a fresh set of disks (see restore-vm.yaml header for the snapshot-name lookup).
kubectl apply -f restore-vm.yaml
```

> Applying `vm1-disks.yaml` and `vm2-disks.yaml` back-to-back starts two host-assisted clones that both read
> the RWO golden. If they land on different nodes they will contend on it; apply the second VM's disks after
> the first VM's root clone reports `Succeeded` (`kubectl get datavolume fedora-vm-1-root -w`), or give the
> golden multi-node read access.

## Group snapshot and restore

`volume-group-snapshot.yaml` selects the group's PVCs by label and produces a single crash-consistent LINSTOR
snapshot of the shared resource, surfaced as one `VolumeSnapshot` per member.

Restore is the mirror of creation (`restore-vm.yaml`): a set of new PVCs, each with a member `VolumeSnapshot`
as its data source **and a new consistency-group label**. The first PVC to be provisioned restores the
whole resource; each member then claims the volume number recorded in its snapshot. A restored group must be
**all-restored** (do not mix restored and fresh members in the same target group).

## Caveats

* **One StorageClass per group.** All members share a single resource definition, resource group, and
  placement. Give every disk of a VM the same StorageClass; members that disagree on placement, layer list,
  storage pool, or encryption contradict the first member.
* **Not compatible with RWX-over-NFS.** The NFS/ganesha RWX path commandeers volume numbers within a resource;
  the driver rejects consistency-group members that request it. RWX **block** (KubeVirt live migration) is
  supported: the driver turns on DRBD two-primaries on demand while the volume is briefly open on two nodes, so
  the StorageClass need not set it.
* **No content source on a member, except a group snapshot.** Restoring or cloning *one* member into a
  standalone volume is not supported (LINSTOR restore is whole-resource). A group member accepts a content
  source **only** in the form of a group-snapshot restore (`restore-vm.yaml`); a plain CSI clone is rejected —
  which is why the golden image is cloned with a host-assisted copy (see above), not a smart clone.
* **Stamping the label is manual.** Nothing stamps the label onto a VM's disk PVCs automatically.
  Label the PVCs yourself (as in `vm1-disks.yaml`) or use a mutating webhook.

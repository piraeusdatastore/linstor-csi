# Linstor CSI Plugin

This CSI plugin allows for the use of LINSTOR volumes on Container Orchestrators
that implement CSI, such as Kubernetes.


# Building

This project is written in Go. If you haven't built a Go program before,
please refer to this [helpful guide](https://golang.org/doc/install).

Requires Go 1.10 or higher and a configured GOPATH, once that is is done. Please
ensure that this project is cloned into the proper directory for the go tools
(`$GOPATH/github.com/LINBIT/linstor-csi/`) and run `make`.

This will create a binary named `linstor-csi` in the root of the project.

# Deployment

## Kubernetes

 Kubernetes deployment examples forthcoming.

# Usage

This project must be used in conjunction with a working LINSTOR cluster. [LINSTOR's
documentation](https://docs.linbit.com/docs/users-guide-9.0/#p-linstor) is the
foremost guide on setting up and administering LINSTOR.

## Kubernetes

After the provisioner has been deployed you're free to create storage classes
that point to the name of the external provisioner associateed with the CSI plugin
and have your users start provisioning volumes from them. Please see
the class.yaml and pvc.yaml files in the example dir for examples.

On the successful creation of a PV, a new linstor resource with the same name as the
PV is created as well.

# License

GPL 2

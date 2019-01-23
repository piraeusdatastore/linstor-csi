# Linstor CSI Plugin

This CSI plugin allows for the use of LINSTOR volumes on Container Orchestrators
that implement CSI, such as Kubernetes.

# Building

This project is written in Go. If you haven't built a Go program before,
please refer to this [helpful guide](https://golang.org/doc/install).

Requires Go 1.11 or higher and a configured GOPATH, once that is is done. Please
ensure that this project is cloned into the proper directory for the go tools
(`$GOPATH/github.com/LINBIT/linstor-csi/`) and run `make`.

This will create a binary named `linstor-csi` in the root of the project.

# Deployment

## Kubernetes

The yaml file in `examples/k8s/deploy` shows an example configuration which
will deploy the LINSTOR csi plugin along with the needed k8s sidecar containers.
You will need to change all instances of `LINSTOR_IP` to point to the controller(s)
of the LINSTOR cluster that you wish this plugin to interact with.

You will need to enable the following feature gates on both the kube-apiserver
and all kubelets for this plugin to be operational: `CSINodeInfo=true`,
`CSIDriverRegistry=true`. Please ensure that your version of Kubernetes is
recent enough to enable these gates.

# Usage

This project must be used in conjunction with a working LINSTOR cluster. [LINSTOR's
documentation](https://docs.linbit.com/docs/users-guide-9.0/#p-linstor) is the
foremost guide on setting up and administering LINSTOR.

## Kubernetes

After the plugin has been deployed you're free to create storage classes
that point to the name of the external provisioner associateed with the CSI plugin
and have your users start provisioning volumes from them. Please see
the class.yaml file in the `examples/k8s/` dir for a basic example.

Ensure that all kubelets that are expected to use LINSTOR volumes have a running
LINSTOR satellite that is configured to work with the LINSTOR controller
configured in the plugin's deployment files and that the storage pool indicated
in the storage class has been properly configured. This pool does not need to be
present on the Kubelets themselves.

# License

GPL 2

module github.com/piraeusdatastore/linstor-csi

go 1.12

require (
	github.com/LINBIT/golinstor v0.16.2
	github.com/LINBIT/linstor-csi v0.7.4
	github.com/container-storage-interface/spec v1.1.0
	github.com/golang/protobuf v1.3.1
	github.com/haySwim/data v0.2.0
	github.com/kubernetes-csi/csi-test v2.0.1+incompatible
	github.com/pborman/uuid v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.3.0
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys v0.0.0-20190613124609-5ed2794edfdc
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.19.0
	k8s.io/api v0.0.0-20190602205700-9b8cae951d65
	k8s.io/apimachinery v0.0.0-20190602113612-63a6072eb563
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/kubernetes v1.14.10
)

// fix for https://github.com/kubernetes/client-go/issues/584
replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190313205120-d7deff9243b1

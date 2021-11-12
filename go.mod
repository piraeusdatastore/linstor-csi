module github.com/piraeusdatastore/linstor-csi

go 1.12

require (
	github.com/LINBIT/golinstor v0.37.1
	github.com/alvaroloes/enumer v1.1.2
	github.com/container-storage-interface/spec v1.4.0
	github.com/golang/protobuf v1.4.3
	github.com/haySwim/data v0.2.0
	github.com/kubernetes-csi/csi-test/v4 v4.2.0
	github.com/pborman/uuid v1.2.0
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.6.1
	github.com/vektra/mockery/v2 v2.8.0
	golang.org/x/sys v0.0.0-20210426230700-d19ff857e887
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.34.0
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c
	k8s.io/api v0.21.2
	k8s.io/apimachinery v0.21.2
	k8s.io/client-go v0.21.2
	k8s.io/mount-utils v0.21.2
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920
)

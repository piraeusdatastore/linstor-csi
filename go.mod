module github.com/LINBIT/linstor-csi

go 1.12

require (
	github.com/LINBIT/golinstor v0.16.2
	github.com/container-storage-interface/spec v0.3.0
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/golang/protobuf v1.3.1
	github.com/haySwim/data v0.2.0
	github.com/kubernetes-csi/csi-test v0.3.0-1
	github.com/pborman/uuid v1.2.0
	github.com/pkg/errors v0.8.1 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/stretchr/testify v1.3.0
	golang.org/x/crypto v0.0.0-20190611184440-5c40567a22f8 // indirect
	golang.org/x/net v0.0.0-20190613194153-d28f0bde5980 // indirect
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys v0.0.0-20190613124609-5ed2794edfdc // indirect
	golang.org/x/text v0.3.2 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20190613204242-ed0dc450797f // indirect
	google.golang.org/grpc v1.19.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.0.0-20190602205700-9b8cae951d65
	k8s.io/apimachinery v0.0.0-20190602113612-63a6072eb563
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/kube-openapi v0.0.0-20190603182131-db7b694dc208 // indirect
	k8s.io/kubernetes v1.14.2
	k8s.io/utils v0.0.0-20190607212802-c55fbcfc754a // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

// fix for https://github.com/kubernetes/client-go/issues/584
replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190313205120-d7deff9243b1

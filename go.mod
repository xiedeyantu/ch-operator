module github.com/xiedeyantu/ch-operator

go 1.13

require (
	github.com/go-logr/logr v0.1.0
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/operator-framework/operator-sdk v0.17.0
	github.com/spf13/pflag v1.0.5
	golang.org/x/lint v0.0.0-20191125180803-fdd1cda4f05f // indirect
	golang.org/x/tools v0.0.0-20200331202046-9d5940d49312 // indirect
	k8s.io/api v0.17.5
	k8s.io/apimachinery v0.17.5
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/code-generator v0.17.5
	sigs.k8s.io/controller-runtime v0.5.2
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.2+incompatible // Required by OLM
	k8s.io/api => k8s.io/api v0.17.5
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.6-beta.0
	k8s.io/client-go => k8s.io/client-go v0.17.5

)

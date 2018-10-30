package e2e_test

import (
	"flag"
	"path/filepath"
	"testing"
	"time"

	"github.com/appscode/go/homedir"
	"github.com/appscode/go/log"
	logs "github.com/appscode/go/log/golog"
	catalog "github.com/kubedb/apimachinery/apis/catalog/v1alpha1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	cs "github.com/kubedb/apimachinery/client/clientset/versioned"
	"github.com/kubedb/apimachinery/client/clientset/versioned/scheme"
	"github.com/kubedb/redis/pkg/controller"
	"github.com/kubedb/redis/test/e2e/framework"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/labels"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	clientSetScheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
	ka "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	appcat_cs "kmodules.xyz/custom-resources/client/clientset/versioned/typed/appcatalog/v1alpha1"
)

// To Run E2E tests:
//
// 1. ./hack/make.py test e2e
//
// 2. ./hack/make.py test e2e --v=1  --docker-registry=kubedbci --db-catalog=5.0 --db-version=5.0 --selfhosted-operator=true
type clusterVar struct {
	f                   *framework.Invocation
	redis               *api.Redis
	redisVersion        *catalog.RedisVersion
	redisInstanceNumber int
	selector            labels.Set
}

var (
	storageClass = "standard"
)

func init() {
	scheme.AddToScheme(clientSetScheme.Scheme)

	flag.StringVar(&storageClass, "storageclass", storageClass, "Kubernetes StorageClass name")
	flag.StringVar(&framework.DockerRegistry, "docker-registry", framework.DockerRegistry, "User provided docker repository")
	flag.StringVar(&framework.ExporterTag, "exporter-tag", framework.ExporterTag, "Tag of kubedb/operator used as exporter")
	flag.StringVar(&framework.DBCatalogName, "db-catalog", framework.DBCatalogName, "Postgres version")
	flag.StringVar(&framework.DBVersion, "db-version", framework.DBVersion, "Redis version")
	flag.BoolVar(&framework.SelfHostedOperator, "selfhosted-operator", framework.SelfHostedOperator, "Enable this for provided controller")
	flag.BoolVar(&framework.Cluster, "cluster", framework.Cluster, "Enable cluster tests")
}

const (
	TIMEOUT = 20 * time.Minute
)

var (
	ctrl *controller.Controller
	root *framework.Framework
	cl   clusterVar
)

func TestE2e(t *testing.T) {
	logs.InitLogs()
	defer logs.FlushLogs()
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(TIMEOUT)

	junitReporter := reporters.NewJUnitReporter("junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "e2e Suite", []Reporter{junitReporter})
}

var _ = BeforeSuite(func() {

	userHome := homedir.HomeDir()

	// Kubernetes config
	kubeconfigPath := filepath.Join(userHome, ".kube/config")
	By("Using kubeconfig from " + kubeconfigPath)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	// raise throttling time. ref: https://github.com/appscode/voyager/issues/640
	config.Burst = 100
	config.QPS = 100

	// Clients
	kubeClient := kubernetes.NewForConfigOrDie(config)
	extClient := cs.NewForConfigOrDie(config)
	kaClient := ka.NewForConfigOrDie(config)
	appCatalogClient, err := appcat_cs.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	}
	// Framework
	root = framework.New(config, kubeClient, extClient, kaClient, appCatalogClient, storageClass)

	// Create namespace
	By("Using namespace " + root.Namespace())
	err = root.CreateNamespace()
	Expect(err).NotTo(HaveOccurred())

	if !framework.SelfHostedOperator {
		stopCh := genericapiserver.SetupSignalHandler()
		go root.RunOperatorAndServer(config, kubeconfigPath, stopCh)
	}

	root.EventuallyCRD().Should(Succeed())
	root.EventuallyAPIServiceReady().Should(Succeed())

	if framework.Cluster {
		cl = clusterVar{}
		cl.f = root.Invoke()
		cl.redis = cl.f.RedisCluster()
		cl.redisVersion = cl.f.RedisVersion()
		createAndWaitForRunning()
	}
})

var _ = AfterSuite(func() {
	if framework.Cluster {
		deleteTestResource()
	}

	By("Cleanup Left Overs")
	if !framework.SelfHostedOperator {
		By("Delete Admission Controller Configs")
		root.CleanAdmissionConfigs()
	}
	By("Delete left over Redis objects")
	root.CleanRedis()
	By("Delete left over Dormant Database objects")
	root.CleanDormantDatabase()
	By("Delete Namespace")
	err := root.DeleteNamespace()
	Expect(err).NotTo(HaveOccurred())
})

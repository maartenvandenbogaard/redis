package configure_cluster

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	KubeClient kubernetes.Interface

	BaseName         string
	Namespace        string
	GoverningService string

	Cluster RedisCluster
}

type RedisCluster struct {
	MasterCnt int
	Replicas  int
}

type RedisNode struct {
	SlotStart []int
	SlotEnd   []int
	SlotsCnt  int

	Id   string
	Ip   string
	Port int
	Role string
	Down bool

	Master *RedisNode
	Slaves []*RedisNode
}

func getConfig() Config {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		panic(err)
	}

	baseName := os.Getenv("BASE_NAME")
	namespace := ""
	if data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		namespace = strings.TrimSpace(string(data))
	}
	if len(namespace) > 0 {
		namespace = "default"
	}
	//namespace := os.Getenv("NAMESPACE")
	//if namespace == "" {
	//	namespace = "default"
	//}
	governingService := os.Getenv("GOVERNING_SERVICE")
	if governingService == "" {
		governingService = "kubedb"
	}
	masterCnt, _ := strconv.Atoi(os.Getenv("MASTER_COUNT"))
	replicas, _ := strconv.Atoi(os.Getenv("REPLICAS"))

	return Config{
		KubeClient:       kubeClient,
		BaseName:         baseName,
		Namespace:        namespace,
		GoverningService: governingService,
		Cluster: RedisCluster{
			MasterCnt: masterCnt,
			Replicas:  replicas,
		},
	}
}

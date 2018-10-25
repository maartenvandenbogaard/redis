package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tamalsaha/go-oneliners"

	"github.com/appscode/go/log"
	//kerr "k8s.io/apimachinery/pkg/api/errors"
	"time"

	core_util "github.com/appscode/kutil/core/v1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/redis/pkg/exec"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	RestConfig *rest.Config
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

	ID   string
	IP   string
	Port int
	Role string
	Down bool

	Master *RedisNode
	Slaves []*RedisNode
}

func ConfigureRedisCluster(
	restConfig *rest.Config, kubeClient kubernetes.Interface, redis *api.Redis, pods [][]*core.Pod) error {
	config := Config{
		RestConfig: restConfig,
		KubeClient: kubeClient,

		BaseName:  redis.Name,
		Namespace: redis.Namespace,
		Cluster: RedisCluster{
			MasterCnt: int(*redis.Spec.Cluster.Master),
			Replicas:  int(*redis.Spec.Cluster.Replicas),
		},
	}

	if err := config.waitUntillRedisServersToBeReady(pods); err != nil {
		return err
	}
	if err := config.configureClusterState(pods); err != nil {
		return err
	}

	return nil
}

func (c Config) getInstances() ([][]*core.Pod, error) {
	pods := make([][]*core.Pod, c.Cluster.MasterCnt)
	for i := 0; i < c.Cluster.MasterCnt; i++ {
		pods[i] = []*core.Pod{}

		for j := 0; j <= c.Cluster.Replicas; j++ {
			podName := fmt.Sprintf("%s-shard%d-%d", c.BaseName, i, j)

			{
				_, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
				//	for default/redis0-shard0-0 >>>>>>>>>>>>>>>
				//%!(EXTRA string=pods "redis0-shard0-0" is forbidden: User "system:serviceaccount:default:default" cannot get pods in the namespace "default")__FILE__
				if err != nil {
					fmt.Printf("for %s/%s >>>>>>>>>>>>>>>\n", c.Namespace, podName)
					oneliners.PrettyJson(err)
				}
			}

			if err := core_util.WaitUntilPodRunning(
				c.KubeClient,
				metav1.ObjectMeta{
					Name:      podName,
					Namespace: c.Namespace,
				},
			); err != nil {
				return nil, errors.Wrapf(err, "failed to get pod '%s/%s'", c.Namespace, podName)
			}

			log.Infof("%s/%s is ready", c.Namespace, podName)
			pod, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			pods[i] = append(pods[i], pod)
		}
	}

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		fmt.Println("[")
		for j := 0; j <= c.Cluster.Replicas; j++ {
			fmt.Printf("\t %s, %s\n", pods[i][j].Name, pods[i][j].Status.PodIP)
		}
		fmt.Println("]")
	}
	return pods, nil
}

func (c Config) createCluster(pod *core.Pod, addrs ...string) error {
	e := exec.NewExecWithInputOptions(c.RestConfig, c.KubeClient, "yes")
	out, err := e.Run(pod, ClusterCreateCmd(0, addrs...)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to create cluster using (%v)", addrs)
	}

	fmt.Println(out)
	return nil
}

func (c Config) addNode(pod *core.Pod, newAddr, existingAddr, masterId string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	var (
		out string
		err error
	)

	if masterId == "" {
		if out, err = e.Run(pod, AddNodeAsMasterCmd(newAddr, existingAddr)...); err != nil {
			return errors.Wrapf(err, "Failed to add %q as a master", newAddr)
		}
	} else {
		if out, err = e.Run(pod, AddNodeAsSlaveCmd(newAddr, existingAddr, masterId)...); err != nil {
			return errors.Wrapf(err, "Failed to add %q as a slave of master with id %q", newAddr, masterId)
		}
	}

	fmt.Println(out)
	return nil
}

func (c Config) deleteNode(pod *core.Pod, existingAddr, deletingNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	out, err := e.Run(pod, DeleteNodeCmd(existingAddr, deletingNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to delete node with ID %q", deletingNodeID)
	}

	fmt.Println(out)
	return nil
}

func (c Config) ping(pod *core.Pod, ip string) (string, error) {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	pong, err := e.Run(pod, PingCmd(ip)...)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to ping %q", pod.Status.PodIP)
	}

	return strings.TrimSpace(pong), nil
}

func (c Config) migrateKey(pod *core.Pod, srcNodeIP, dstNodeIP, dstNodePort, key, dbID, timeout string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, MigrateKeyCmd(srcNodeIP, dstNodeIP, dstNodePort, key, dbID, timeout)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to migrate key %q from %q to %q", key, pod.Status.PodIP, dstNodeIP)
	}

	//fmt.Println(out)
	return nil
}

func (c Config) getClusterNodes(pod *core.Pod, ip string) (string, error) {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	out, err := e.Run(pod, ClusterNodesCmd(ip)...)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get cluster nodes from %q", ip)
	}

	return strings.TrimSpace(out), nil
}

func (c Config) clusterReset(pod *core.Pod, ip, option string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	out, err := e.Run(pod, ClusterResetCmd(ip)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to reset node %q", ip)
	}

	fmt.Println(out)
	return nil
}

func (c Config) clusterFailover(pod *core.Pod, ip string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	out, err := e.Run(pod, ClusterFailoverCmd(ip)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to failover node %q", ip)
	}

	fmt.Println(out)
	return nil
}

func (c Config) clusterSetSlotImporting(pod *core.Pod, dstNodeIP, slot, srcNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotImportingCmd(dstNodeIP, slot, srcNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in destination node %q as 'importing' from source node with ID %q",
			slot, dstNodeIP, srcNodeID)
	}

	//fmt.Println(out)
	return nil
}

func (c Config) clusterSetSlotMigrating(pod *core.Pod, srcNodeIP, slot, dstNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotMigratingCmd(srcNodeIP, slot, dstNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in source node %q as 'migrating' to destination node with ID %q",
			slot, srcNodeIP, dstNodeID)
	}

	//fmt.Println(out)
	return nil
}

func (c Config) clusterSetSlotNode(pod *core.Pod, toNodeIP, slot, dstNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotNodeCmd(toNodeIP, slot, dstNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in node %q as 'node' to destination node with ID %q",
			slot, toNodeIP, dstNodeID)
	}

	//fmt.Println(out)
	return nil
}

func (c Config) clusterGetKeysInSlot(pod *core.Pod, srcNodeIP, slot string) (string, error) {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	out, err := e.Run(pod, ClusterGetKeysInSlotCmd(srcNodeIP, slot)...)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get key at slot %q from node %q",
			slot, srcNodeIP)
	}

	return strings.TrimSpace(out), nil
}

func (c Config) clusterReplicate(pod *core.Pod, receivingNodeIP, masterNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterReplicateCmd(receivingNodeIP, masterNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to replicate node %q of node with ID %s",
			receivingNodeIP, masterNodeID)
	}

	return nil
}

func (c Config) reshard(
	pod *core.Pod, nodes [][]RedisNode, src, dst, requstedSlotsCount int) error {
	//cmd := NewCmdWithDefaultOptions()
	//out, err := cmd.Run("redis-trib",
	//	[]string{"reshard", "--from", from, "--to", to, "--slots", slots, "--yes"}...)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Println(out)
	//
	log.Infof("resharding %d slots from %q to %q...\n\n\n",
		requstedSlotsCount, nodes[src][0].IP, nodes[dst][0].IP)

	var (
		movedSlotsCount int
		err             error
	)
	movedSlotsCount = 0
	need := requstedSlotsCount
Reshard:
	for i := range nodes[src][0].SlotStart {
		if movedSlotsCount >= requstedSlotsCount {
			break Reshard
		}

		start := nodes[src][0].SlotStart[i]
		end := nodes[src][0].SlotEnd[i]
		// ==================
		en := end
		if en-start+1 > need {
			en = start + need - 1
		}
		cmd := []string{"/conf/cluster.sh", "reshard", nodes[src][0].IP, nodes[src][0].ID, nodes[dst][0].IP, nodes[dst][0].ID,
			strconv.Itoa(start), strconv.Itoa(en),
		}
		for k := range nodes {
			if k != src && k != dst {
				cmd = append(cmd, nodes[k][0].IP)
			}
		}

		e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
		_, err = e.Run(pod, cmd...)
		if err != nil {
			return errors.Wrapf(err, "Failed to reshard %d slots from %q to %q",
				requstedSlotsCount, nodes[src][0].IP, nodes[dst][0].IP)
		}
		return nil
		// ==================
		//for slot := start; slot <= end; slot++ {
		//	if movedSlotsCount >= requstedSlotsCount {
		//		break Reshard
		//	}
		//
		//	if err = c.clusterSetSlotImporting(pod, nds[dstNodeID].IP, strconv.Itoa(slot), srcNodeID); err != nil {
		//		return err
		//	}
		//	if err = c.clusterSetSlotMigrating(pod, nds[srcNodeID].IP, strconv.Itoa(slot), dstNodeID); err != nil {
		//		return err
		//	}
		//	for {
		//		key, err := c.clusterGetKeysInSlot(pod, nds[srcNodeID].IP, strconv.Itoa(slot))
		//		if err != nil {
		//			return err
		//		}
		//		if key == "" {
		//			break
		//		}
		//		if err = c.migrateKey(
		//			pod, nds[srcNodeID].IP, nds[dstNodeID].IP, strconv.Itoa(nds[dstNodeID].Port), key,
		//			"0", "5000"); err != nil {
		//			return err
		//		}
		//	}
		//	if err = c.clusterSetSlotNode(pod, nds[srcNodeID].IP, strconv.Itoa(slot), dstNodeID); err != nil {
		//		return err
		//	}
		//	if err = c.clusterSetSlotNode(pod, nds[dstNodeID].IP, strconv.Itoa(slot), dstNodeID); err != nil {
		//		return err
		//	}
		//
		//	for masterId, master := range nds {
		//		if masterId != srcNodeID && masterId != dstNodeID {
		//			if err = c.clusterSetSlotNode(pod, master.IP, strconv.Itoa(slot), dstNodeID); err != nil {
		//				return err
		//			}
		//		}
		//	}
		//	movedSlotsCount++
		//}
	}

	return nil
}

func getMyConf(nodesConf string) (myConf string) {
	myConf = ""
	nodes := strings.Split(nodesConf, "\n")
	for _, node := range nodes {
		if strings.Contains(node, "myself") {
			myConf = strings.TrimSpace(node)
			break
		}
	}

	return myConf
}

func getNodeConfByIP(nodesConf, ip string) (myConf string) {
	myConf = ""
	nodes := strings.Split(nodesConf, "\n")
	for _, node := range nodes {
		if strings.Contains(node, ip) {
			myConf = strings.TrimSpace(node)
			break
		}
	}

	return myConf
}

func getNodeId(nodeConf string) string {
	return strings.Split(nodeConf, " ")[0]
}

func getNodeRole(nodeConf string) (nodeRole string) {
	nodeRole = ""
	if strings.Contains(nodeConf, "master") {
		nodeRole = "master"
	} else if strings.Contains(nodeConf, "slave") {
		nodeRole = "slave"
	}

	return nodeRole
}

func getMasterID(nodeConf string) (masterID string) {
	masterID = ""
	if getNodeRole(nodeConf) == "slave" {
		masterID = strings.Split(nodeConf, " ")[3]
	}

	return masterID
}

func (c Config) waitUntillRedisServersToBeReady(pods [][]*core.Pod) error {
	//pods, err := c.getInstances()
	//if err != nil {
	//	return err
	//}

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		for j := 0; j <= c.Cluster.Replicas; j++ {
			//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, j, c.GoverningService, c.Namespace))
			for {
				if pong, _ := c.ping(pods[i][j], pods[i][j].Status.PodIP); pong == "PONG" {
					log.Infof("%s is ready", pods[i][j].Status.PodIP)
					break
				}
			}
		}
	}

	return nil
}

func processNodesConf(nodesConf string) map[string]*RedisNode {
	var (
		slotRange  []string
		start, end int
		nds        map[string]*RedisNode
	)

	nds = make(map[string]*RedisNode)
	nodes := strings.Split(nodesConf, "\n")

	for _, node := range nodes {
		node = strings.TrimSpace(node)
		parts := strings.Split(strings.TrimSpace(node), " ")

		if strings.Contains(parts[2], "noaddr") {
			continue
		}

		if strings.Contains(parts[2], "master") {
			nd := RedisNode{
				ID:   parts[0],
				IP:   strings.Split(parts[1], ":")[0],
				Port: 6379,
				Role: "master",
				Down: false,
			}
			if strings.Contains(parts[2], "fail") {
				nd.Down = true
			}
			nd.SlotsCnt = 0
			for j := 8; j < len(parts); j++ {
				if parts[j][0] == '[' && parts[j][len(parts[j])-1] == ']' {
					continue
				}

				slotRange = strings.Split(parts[j], "-")
				start, _ = strconv.Atoi(slotRange[0])
				if len(slotRange) == 1 {
					end = start
				} else {
					end, _ = strconv.Atoi(slotRange[1])
				}

				nd.SlotStart = append(nd.SlotStart, start)
				nd.SlotEnd = append(nd.SlotEnd, end)
				nd.SlotsCnt += (end - start) + 1
			}
			nd.Slaves = []*RedisNode{}

			nds[nd.ID] = &nd
		}
	}

	for _, node := range nodes {
		node = strings.TrimSpace(node)
		parts := strings.Split(strings.TrimSpace(node), " ")

		if strings.Contains(parts[2], "noaddr") {
			continue
		}

		if strings.Contains(parts[2], "slave") {
			nd := RedisNode{
				ID:   parts[0],
				IP:   strings.Split(parts[1], ":")[0],
				Port: 6379,
				Role: "slave",
				Down: false,
			}
			if strings.Contains(parts[2], "fail") {
				nd.Down = true
			}
			nd.Master = nds[parts[3]]
			nds[parts[3]].Slaves = append(nds[parts[3]].Slaves, &nd)
		}
	}

	//for masterId, master := range nds {
	//	fmt.Println(">>>>>>>> masterId =", masterId)
	//	fmt.Println("=============================================================")
	//	fmt.Println("{")
	//	fmt.Println("\t ID:", masterId)
	//	fmt.Println("\t IP:", master.IP)
	//	fmt.Println("\t Role:", master.Role)
	//	fmt.Println("\t Down:", master.Down)
	//	fmt.Println("\t Slot Count:", master.SlotsCnt)
	//	fmt.Println("\t Slot Start:", master.SlotStart)
	//	fmt.Println("\t Slot End:", master.SlotEnd)
	//	for _, slave := range master.Slaves {
	//		fmt.Println("\t{")
	//		fmt.Println("\t\t ID:", slave.ID)
	//		fmt.Println("\t\t IP:", slave.IP)
	//		fmt.Println("\t\t Role:", slave.Role)
	//		fmt.Println("\t\t Down:", slave.Down)
	//		fmt.Println("\t\t MasterId =", slave.Master.ID)
	//		fmt.Println("\t}")
	//	}
	//	fmt.Println("}")
	//}

	return nds
}

func (c Config) ensureFirstPodAsMaster(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring 1st pod as master in each statefulSet...")

	var (
		//pods      [][]*core.Pod
		err       error
		nodesConf string
	)

	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, 0, 0, c.GoverningService, c.Namespace))
	if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
		return err
	}

	if strings.Count(nodesConf, "master") > 1 {
		wait := false
		for i := 0; i < c.Cluster.MasterCnt; i++ {
			if nodesConf, err = c.getClusterNodes(pods[i][0], pods[i][0].Status.PodIP); err != nil {
				return err
			}
			if getNodeRole(getMyConf(nodesConf)) != "master" {
				if err = c.clusterFailover(pods[i][0], pods[i][0].Status.PodIP); err != nil {
					return err
				}
				wait = true
			}
		}
		if wait {
			log.Infoln("Waiting to update cluster for ensuring 1st pod as master...")
			time.Sleep(time.Minute)
		}
	}

	return nil
}

func (c Config) getOrderedNodes(pods [][]*core.Pod) ([][]RedisNode, error) {
	var (
		//pods         [][]*core.Pod
		err          error
		nodesConf    string
		nodes        map[string]*RedisNode
		orderedNodes [][]RedisNode
	)

	if err = c.ensureFirstPodAsMaster(pods); err != nil {
		return nil, err
	}

	//if pods, err = c.getInstances(); err != nil {
	//	return nil, nil, err
	//}
Again:
	for {
		if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
			return nil, err
		}
		nodes = processNodesConf(nodesConf)
		for _, master := range nodes {
			log.Infoln("master = ", master.IP)
			for i := 0; i < len(pods); i++ {
				if pods[i][0].Status.PodIP == master.IP {
					log.Infoln("found master = ", master.IP, " at ", i)
					for _, slave := range master.Slaves {
						log.Infoln("\tslave = ", slave.IP)
						for k := 0; k < len(pods); k++ {
							for j := 1; j < len(pods[k]); j++ {
								if pods[k][j].Status.PodIP == slave.IP && i != k {
									log.Infoln("\tfound slave = ", slave.IP, " at ", k, " ", j)
									if err = c.clusterReplicate(
										pods[k][j], pods[k][j].Status.PodIP,
										getNodeId(getNodeConfByIP(nodesConf, pods[k][0].Status.PodIP))); err != nil {
										return nil, err
									}
									time.Sleep(time.Second * 5)
									log.Infoln("Again")
									goto Again
								}
							}
						}
					}
					break
				}
			}
		}
		break
	}

	log.Infoln("pods len = ", len(pods))
	for i := range pods {
		log.Infoln("pods[", i, "] len = ", len(pods[i]))
	}
	log.Infoln("nodes len = ", len(nodes))
	//for i, master := range nodes {
	//	fmt.Println(">>>>>>>> index =", i)
	//	fmt.Println("=============================================================")
	//	fmt.Println("{")
	//	fmt.Println("\t ID:", master.ID)
	//	fmt.Println("\t IP:", master.IP)
	//	fmt.Println("\t Role:", master.Role)
	//	fmt.Println("\t Down:", master.Down)
	//	fmt.Println("\t Slot Count:", master.SlotsCnt)
	//	fmt.Println("\t Slot Start:", master.SlotStart)
	//	fmt.Println("\t Slot End:", master.SlotEnd)
	//	for _, slave := range master.Slaves {
	//		fmt.Println("\t{")
	//		fmt.Println("\t\t ID:", slave.ID)
	//		fmt.Println("\t\t IP:", slave.IP)
	//		fmt.Println("\t\t Role:", slave.Role)
	//		fmt.Println("\t\t Down:", slave.Down)
	//		fmt.Println("\t\t MasterId =", slave.Master.ID)
	//		fmt.Println("\t}")
	//	}
	//	fmt.Println("}")
	//}
	orderedNodes = make([][]RedisNode, len(nodes))
	for i := 0; i < len(nodes); i++ {
		for _, master := range nodes {
			if master.IP == pods[i][0].Status.PodIP {
				orderedNodes[i] = make([]RedisNode, len(master.Slaves)+1)
				orderedNodes[i][0] = *master
				//orderedNodes[i].Slaves = make([]*RedisNode, len(pods[i]))

				log.Infof("pods[%d].len = %d", i, len(pods[i]))
				log.Infof("orderedNodes[%d].len = %d", i, len(orderedNodes[i]))
				log.Infof("orderedNodes[%d][0].ip = %s", i, orderedNodes[i][0].IP)

				for j := 1; j < len(orderedNodes[i]); j++ {
					log.Infoln("j is ", j)
					for _, slave := range master.Slaves {
						if slave.IP == pods[i][j].Status.PodIP {
							orderedNodes[i][j] = *slave

							break
						}
					}
				}

				break
			}
		}
	}

	if len(orderedNodes) > 1 {
		for i := range orderedNodes {
			if len(orderedNodes[i]) == 0 {
				continue
			}

			fmt.Println(">>>>>>>> index =", i)
			fmt.Println("=============================================================")
			fmt.Println("{")
			fmt.Println("\t ID:", orderedNodes[i][0].ID)
			fmt.Println("\t IP:", orderedNodes[i][0].IP)
			fmt.Println("\t Role:", orderedNodes[i][0].Role)
			fmt.Println("\t Down:", orderedNodes[i][0].Down)
			fmt.Println("\t Slot Count:", orderedNodes[i][0].SlotsCnt)
			fmt.Println("\t Slot Start:", orderedNodes[i][0].SlotStart)
			fmt.Println("\t Slot End:", orderedNodes[i][0].SlotEnd)
			for j := 1; j < len(orderedNodes[i]); j++ {
				fmt.Println("\t{")
				fmt.Println("\t\t ID:", orderedNodes[i][j].ID)
				fmt.Println("\t\t IP:", orderedNodes[i][j].IP)
				fmt.Println("\t\t Role:", orderedNodes[i][j].Role)
				fmt.Println("\t\t Down:", orderedNodes[i][j].Down)
				fmt.Println("\t\t MasterId =", orderedNodes[i][j].Master.ID)
				fmt.Println("\t}")
			}
			fmt.Println("}")
		}
	}

	return orderedNodes, nil
}

func (c Config) ensureExtraSlavesBeRemoved(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring extra slaves be removed...")

	var (
		err error
		//pods [][]*core.Pod
		//nodesConf string
		//nds       map[string]*RedisNode
		nodes [][]RedisNode
	)
	// =========================
	nodes, err = c.getOrderedNodes(pods)
	for i := range nodes {
		if c.Cluster.Replicas < len(nodes[i])-1 {
			for j := c.Cluster.Replicas + 1; j < len(nodes[i]); j++ {
				if err = c.deleteNode(pods[0][0], nodes[i][0].IP+":6379", nodes[i][j].ID); err != nil {
					return err
				}
				time.Sleep(time.Second * 5)

				//if err = c.clusterReset(pods[0][0], pods[i][j].Status.PodIP, "hard"); err != nil {
				//	return err
				//}
			}
		}
	}

	// todo: wait if slave(s) have been removed
	//time.Sleep(time.Second * 30)
	// =========================

	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	////ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, 0, 0, c.GoverningService, c.Namespace))
	////ip = pods[0][0].Status.PodIP
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//
	//nds = processNodesConf(nodesConf)
	//// remove slave(s)
	//for _, master := range nds {
	//	if c.Cluster.Replicas < len(master.Slaves) {
	//		var (
	//			runningSlavesIPs sets.String
	//			masterfound      bool
	//			masterIndex int
	//		)
	//
	//		// find slaves' ips of this master those need to be keep alive
	//		for i := 0; i < c.Cluster.MasterCnt; i++ {
	//			runningSlavesIPs = sets.NewString()
	//			masterfound = false
	//
	//			for j := 0; j <= c.Cluster.Replicas; j++ {
	//				//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	//				//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	//				//curIP := pods[i][j].Status.PodIP
	//				if pods[i][j].Status.PodIP == master.IP {
	//					masterfound = true
	//					masterIndex = i
	//					continue
	//				}
	//				runningSlavesIPs.Insert(pods[i][j].Status.PodIP)
	//			}
	//
	//			if masterfound {
	//				break
	//			}
	//		}
	//
	//		// delete the slaves those aren't in the set 'runningSlavesIps'
	//		for j := c.Cluster.Replicas + 1; j <= len(master.Slaves); j++ {
	//			podName := fmt.Sprintf("%s-shard%d-%d", c.BaseName, masterIndex, j)
	//			pod, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
	//			if err != nil {
	//				return errors.Wrapf(err, "failed to get slave '%s/%s' to delete", c.Namespace, podName)
	//			}
	//
	//			for _, slave := range master.Slaves {
	//				if slave.IP == pod.Status.PodIP {
	//					if err = c.deleteNode(pod, master.IP+":6379", slave.ID); err != nil {
	//						return err
	//					}
	//					if err = c.clusterReset(pod, slave.IP); err != nil {
	//						return err
	//					}
	//				}
	//			}
	//		}
	//		//for _, slave := range master.Slaves {
	//		//	if !runningSlavesIPs.Has(slave.IP) {
	//		//		if err = c.deleteNode(pods[0][0], master.IP+":6379", slave.ID); err != nil {
	//		//			return err
	//		//		}
	//		//		if err = c.clusterReset(pods[0][0], slave.IP); err != nil {
	//		//			return err
	//		//		}
	//		//	}
	//		//}
	//		time.Sleep(time.Second * 20)
	//	}
	//}

	return nil
}

func (c Config) ensureExtraMastersBeRemoved(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring extra masters be removed...")

	var (
		err error
		//pods [][]*core.Pod
		//nodesConf                               string
		//nds                                     map[string]*RedisNode
		existingMasterCnt int
		//masterIDsToBeRemoved, masterIDsToBeKept sets.String //[]string
		slotsPerMaster, slotsRequired int //, allocatedSlotsCnt int
		nodes                         [][]RedisNode
	)
	// =========================
	nodes, err = c.getOrderedNodes(pods)
	existingMasterCnt = len(nodes)
	log.Infoln("existing master count = ", existingMasterCnt)

	if existingMasterCnt > c.Cluster.MasterCnt {
		slotsPerMaster = 16384 / c.Cluster.MasterCnt

		for i := 0; i < c.Cluster.MasterCnt; i++ {
			slotsRequired = slotsPerMaster
			if i == c.Cluster.MasterCnt-1 {
				// this change is only for the last master that needs slots
				slotsRequired = 16384 - (slotsPerMaster * i)
			}

			to := nodes[i][0]
			for k := c.Cluster.MasterCnt; k < existingMasterCnt; k++ {
				from := nodes[k][0]
				// compare with slotsRequired
				if to.SlotsCnt < slotsRequired {
					// But compare with slotsPerMaster. Existing masters always need slots equal to
					// slotsPerMaster not slotsRequired since slotsRequired may change for last master
					// that is being added.
					if from.SlotsCnt > 0 {
						slots := from.SlotsCnt
						if slots > slotsRequired-to.SlotsCnt {
							slots = slotsRequired - to.SlotsCnt
						}

						if err = c.reshard(pods[0][0], nodes, k, i, slots); err != nil {
							return err
						}
						time.Sleep(time.Second * 5)
						to.SlotsCnt += slots
						from.SlotsCnt -= slots
					}
				} else {
					break
				}
			}
		}

		// todo: wait if resharding happens
		//time.Sleep(time.Second * 30)

		for i := c.Cluster.MasterCnt; i < existingMasterCnt; i++ {
			for j := 1; j < len(nodes[i]); j++ {
				if err = c.deleteNode(pods[0][0], nodes[i][0].IP+":6379", nodes[i][j].ID); err != nil {
					return err
				}
				time.Sleep(time.Second * 5)

				//if err = c.clusterReset(pods[i][j], pods[i][j].Status.PodIP, "hard"); err != nil {
				//	return err
				//}
			}
			if err = c.deleteNode(pods[0][0], pods[0][0].Status.PodIP+":6379", nodes[i][0].ID); err != nil {
				return err
			}
			time.Sleep(time.Second * 5)

			//if err = c.clusterReset(pods[i][0], pods[i][0].Status.PodIP, "hard"); err != nil {
			//	return err
			//}
		}
	}
	// =========================
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//log.Infoln("existing master count = ", existingMasterCnt)
	//
	//// remove master(s)
	//if existingMasterCnt > c.Cluster.MasterCnt {
	//	masterIDsToBeRemoved = sets.NewString()
	//	masterIDsToBeKept = sets.NewString() //[]string
	//
	//	slotsPerMaster = 16384 / c.Cluster.MasterCnt
	//	for i := 0; i < c.Cluster.MasterCnt; i++ {
	//		//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	//		//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	//		//curIP := pods[i][0].Status.PodIP
	//		if nodesConf, err = c.getClusterNodes(pods[i][0], pods[i][0].Status.PodIP); err != nil {
	//			return err
	//		}
	//		masterIDsToBeKept.Insert(getNodeId(getMyConf(nodesConf)))
	//	}
	//	for masterID := range nds {
	//		if !masterIDsToBeKept.Has(masterID) {
	//			masterIDsToBeRemoved.Insert(masterID)
	//		}
	//	}
	//	log.Infoln("masterIDsWithLessSlots", masterIDsToBeKept)
	//	log.Infoln("masterIDsWithExtraSlots", masterIDsToBeRemoved)
	//
	//	for i, to := range masterIDsToBeKept.UnsortedList() {
	//		slotsRequired = slotsPerMaster
	//		if i == masterIDsToBeKept.Len()-1 {
	//			// this change is only for the last master that needs slots
	//			slotsRequired = 16384 - (slotsPerMaster * i)
	//		}
	//
	//		for _, from := range masterIDsToBeRemoved.UnsortedList() {
	//			// compare with slotsRequired
	//			if nds[to].SlotsCnt < slotsRequired {
	//				// But compare with slotsPerMaster. Existing masters always need slots equal to
	//				// slotsPerMaster not slotsRequired since slotsRequired may change for last master
	//				// that is being added.
	//				if nds[from].SlotsCnt > 0 {
	//					slots := nds[from].SlotsCnt
	//					if slots > slotsRequired-nds[to].SlotsCnt {
	//						slots = slotsRequired - nds[to].SlotsCnt
	//					}
	//
	//					if err = c.reshard(pods[0][0], nds, from, to, slots); err != nil {
	//						return err
	//					}
	//					nds[to].SlotsCnt += slots
	//					nds[from].SlotsCnt -= slots
	//				}
	//			} else {
	//				break
	//			}
	//		}
	//	}
	//
	//	for i := c.Cluster.MasterCnt; i < len(nds); i++ {
	//		for j := 1; j < c.Cluster.Replicas; j++ {
	//			podName := fmt.Sprintf("%s-shard%d-%d", c.BaseName, i, j)
	//			pod, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
	//			if err != nil {
	//				return errors.Wrapf(err, "failed to get master '%s/%s' to delete", c.Namespace, podName)
	//			}
	//
	//			if err = c.deleteNode(pod, nds[id].IP+":6379", slave.ID); err != nil {
	//				return err
	//			}
	//			if err = c.clusterReset(pods[0][0], slave.IP); err != nil {
	//				return err
	//			}
	//		}
	//	}
	//
	//	for _, id := range masterIDsToBeRemoved.UnsortedList() {
	//		for _, slave := range nds[id].Slaves {
	//			if err = c.deleteNode(pods[0][0], nds[id].IP+":6379", slave.ID); err != nil {
	//				return err
	//			}
	//			if err = c.clusterReset(pods[0][0], slave.IP); err != nil {
	//				return err
	//			}
	//		}
	//		if err = c.deleteNode(pods[0][0], pods[0][0].Status.PodIP+":6379", id); err != nil {
	//			return err
	//		}
	//		if err = c.clusterReset(pods[0][0], nds[id].IP); err != nil {
	//			return err
	//		}
	//	}
	//	time.Sleep(time.Minute)
	//}

	return nil
}

func (c Config) ensureNewMastersBeAdded(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring new masters be added...")

	var (
		err error
		//pods [][]*core.Pod
		//nodesConf         string
		existingMasterCnt int
		nodes             [][]RedisNode
	)
	// =========================
	nodes, err = c.getOrderedNodes(pods)
	existingMasterCnt = len(nodes)
	log.Infoln("existing master count = ", existingMasterCnt)

	if existingMasterCnt > 1 {
		// add new master(s)
		if existingMasterCnt < c.Cluster.MasterCnt {
			for i := existingMasterCnt; i < c.Cluster.MasterCnt; i++ {
				if err = c.clusterReset(pods[i][0], pods[i][0].Status.PodIP, "hard"); err != nil {
					return err
				}
				time.Sleep(time.Second * 5)

				if err = c.addNode(
					pods[0][0],
					pods[i][0].Status.PodIP+":6379", pods[0][0].Status.PodIP+":6379", ""); err != nil {
					return err
				}
				time.Sleep(time.Second * 5)
			}
			//time.Sleep(time.Second * 30)

		}
	}
	// =========================
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//existingMasterCnt = strings.Count(nodesConf, "master")
	//
	//if existingMasterCnt > 1 {
	//	// add new master(s)
	//	if existingMasterCnt < c.Cluster.MasterCnt {
	//		for i := existingMasterCnt; i < c.Cluster.MasterCnt; i++ {
	//			//newMasterIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, 0, c.GoverningService, c.Namespace))
	//			//newMasterIp := ips[i][0]
	//			if err = c.addNode(
	//				pods[0][0],
	//				pods[i][0].Status.PodIP+":6379", pods[0][0].Status.PodIP+":6379", ""); err != nil {
	//				return err
	//			}
	//		}
	//		time.Sleep(time.Minute)
	//		//nds = processNodesConf(getClusterNodes(ip))
	//	}
	//}

	return nil
}

func (c Config) rebalanceSlots(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring slots are rebalanced...")

	var (
		err error
		//pods [][]*core.Pod
		//nodesConf                                       string
		//nds                                             map[string]*RedisNode
		existingMasterCnt int
		//masterIDsWithLessSlots, masterIDsWithExtraSlots []string
		masterIndicesWithLessSlots, masterIndicesWithExtraSlots []int
		//nonEmptyMastersId, emptyMastersId                []string
		slotsPerMaster, slotsRequired int //, allocatedSlotsCnt int
		nodes                         [][]RedisNode
	)
	// =========================
	nodes, err = c.getOrderedNodes(pods)

	existingMasterCnt = len(nodes)
	log.Infoln("existing master count = ", existingMasterCnt)

	if existingMasterCnt > 1 {
		slotsPerMaster = 16384 / c.Cluster.MasterCnt
		for i := range nodes {
			if nodes[i][0].SlotsCnt < slotsPerMaster {
				masterIndicesWithLessSlots = append(masterIndicesWithLessSlots, i)
			} else {
				masterIndicesWithExtraSlots = append(masterIndicesWithExtraSlots, i)
			}
		}
		log.Infoln("masterIndicesWithLessSlots", masterIndicesWithLessSlots)
		log.Infoln("masterIndicesWithExtraSlots", masterIndicesWithExtraSlots)

		for i := range masterIndicesWithLessSlots {
			slotsRequired = slotsPerMaster
			if i == len(masterIndicesWithLessSlots)-1 {
				// this change is only for the last master that needs slots
				slotsRequired = 16384 - (slotsPerMaster * i)
			}

			to := nodes[masterIndicesWithLessSlots[i]][0]
			for k := range masterIndicesWithExtraSlots {
				from := nodes[masterIndicesWithExtraSlots[k]][0]
				// compare with slotsRequired
				if to.SlotsCnt < slotsRequired {
					// But compare with slotsPerMaster. Existing masters always need slots equal to
					// slotsPerMaster not slotsRequired since slotsRequired may change for last master
					// that is being added.
					if from.SlotsCnt > slotsPerMaster {
						slots := from.SlotsCnt - slotsPerMaster
						if slots > slotsRequired-to.SlotsCnt {
							slots = slotsRequired - to.SlotsCnt
						}

						if err = c.reshard(pods[0][0], nodes,
							masterIndicesWithExtraSlots[k], masterIndicesWithLessSlots[i], slots); err != nil {
							return err
						}
						time.Sleep(time.Second * 5)
						to.SlotsCnt += slots
						from.SlotsCnt -= slots
					}
				} else {
					break
				}
			}
		}

		// todo: wait if resharding happens
		//time.Sleep(time.Second * 30)
	}
	// =========================
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//log.Infoln("existing master count = ", existingMasterCnt)
	//
	//if existingMasterCnt > 1 {
	//	slotsPerMaster = 16384 / c.Cluster.MasterCnt
	//	for masterID, master := range nds {
	//		if master.SlotsCnt < slotsPerMaster {
	//			masterIDsWithLessSlots = append(masterIDsWithLessSlots, masterID)
	//			//emptyMastersId = append(emptyMastersId, masterId)
	//		} else {
	//			masterIDsWithExtraSlots = append(masterIDsWithExtraSlots, masterID)
	//			//nonEmptyMastersId = append(nonEmptyMastersId, masterId)
	//		}
	//	}
	//	log.Infoln("masterIDsWithLessSlots", masterIDsWithLessSlots)
	//	log.Infoln("masterIDsWithExtraSlots", masterIDsWithExtraSlots)
	//
	//	for i, to := range masterIDsWithLessSlots {
	//		slotsRequired = slotsPerMaster
	//		if i == len(masterIDsWithLessSlots)-1 {
	//			// this change is only for the last master that needs slots
	//			slotsRequired = 16384 - (slotsPerMaster * i)
	//		}
	//
	//		//allocatedSlotsCnt = nds[to].SlotsCnt
	//		for _, from := range masterIDsWithExtraSlots {
	//			// compare with slotsRequired
	//			if nds[to].SlotsCnt < slotsRequired {
	//				// But compare with slotsPerMaster. Existing masters always need slots equal to
	//				// slotsPerMaster not slotsRequired since slotsRequired may change for last master
	//				// that is being added.
	//				if nds[from].SlotsCnt > slotsPerMaster {
	//					slots := nds[from].SlotsCnt - slotsPerMaster
	//					if slots > slotsRequired-nds[to].SlotsCnt {
	//						slots = slotsRequired - nds[to].SlotsCnt
	//					}
	//
	//					if err = c.reshard(pods[0][0], nds, from, to, slots); err != nil {
	//						return err
	//					}
	//					nds[to].SlotsCnt += slots
	//					nds[from].SlotsCnt -= slots
	//				}
	//			} else {
	//				break
	//			}
	//		}
	//	}
	//	time.Sleep(time.Minute)
	//}

	return nil
}

func (c Config) ensureNewSlavesBeAdded(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring new slaves be added...")

	var (
		err error
		//pods [][]*core.Pod
		//nodesConf         string
		//nds               map[string]*RedisNode
		existingMasterCnt int
		//masterFound       bool
		//curSlaves         sets.String
		nodes [][]RedisNode
	)
	// =========================
	nodes, err = c.getOrderedNodes(pods)

	existingMasterCnt = len(nodes)
	log.Infoln("existing master count = ", existingMasterCnt)

	if existingMasterCnt > 1 {
		// add new slave(s)
		for i := range nodes {
			if len(nodes[i])-1 < c.Cluster.Replicas {
				for j := len(nodes[i]); j <= c.Cluster.Replicas; j++ {
					if err = c.clusterReset(pods[i][j], pods[i][j].Status.PodIP, "hard"); err != nil {
						return err
					}
					time.Sleep(time.Second * 5)

					if err = c.addNode(
						pods[0][0],
						pods[i][j].Status.PodIP+":6379", nodes[i][0].IP+":6379", nodes[i][0].ID); err != nil {
						return err
					}
					time.Sleep(time.Second * 5)
				}
			}
		}
		// todo: wait if new slave(s) have been added
		//time.Sleep(time.Second * 30)
	}
	// =========================
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//
	//if existingMasterCnt > 1 {
	//	// add new slave(s)
	//	for masterID, master := range nds {
	//		if len(master.Slaves) < c.Cluster.Replicas {
	//			for i := 0; i < c.Cluster.MasterCnt; i++ {
	//				masterFound = false
	//				curSlaves = sets.NewString()
	//
	//				for j := 0; j <= c.Cluster.Replicas; j++ {
	//					if pods[i][j].Status.PodIP == master.IP {
	//						masterFound = true
	//						for _, slave := range master.Slaves {
	//							curSlaves.Insert(slave.IP)
	//						}
	//						break
	//					}
	//					//slaves.Insert(ips[i][j])
	//				}
	//
	//				if masterFound {
	//					for j := 0; j <= c.Cluster.Replicas; j++ {
	//						if pods[i][j].Status.PodIP != master.IP && !curSlaves.Has(pods[i][j].Status.PodIP) {
	//							if err = c.addNode(
	//								pods[0][0],
	//								pods[i][j].Status.PodIP+":6379", master.IP+":6379", masterID); err != nil {
	//								return err
	//							}
	//						}
	//					}
	//					break
	//				}
	//			}
	//		}
	//	}
	//}

	return nil
}

func (c Config) configureClusterState(pods [][]*core.Pod) error {
	var (
		err error
	)

	if err = c.ensureCluster(pods); err != nil {
		return err
	}

	//if err = c.ensureFirstPodAsMaster(); err != nil {
	//	return err
	//}

	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	////ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, 0, 0, c.GoverningService, c.Namespace))
	//ip = pods[0][0].Status.PodIP
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//
	//nds = processNodesConf(nodesConf)
	//masterCnt := len(nds)
	//// remove slave(s)
	//for _, master := range nds {
	//	if c.Cluster.Replicas < len(master.Slaves) {
	//		var (
	//			runningSlavesIPs sets.String
	//			masterfound      bool
	//		)
	//
	//		// find slaves' ips of this master those need to be keep alive
	//		for i := 0; i < c.Cluster.MasterCnt; i++ {
	//			runningSlavesIPs = sets.NewString()
	//			masterfound = false
	//
	//			for j := 0; j <= c.Cluster.Replicas; j++ {
	//				//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	//				//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	//				curIP := pods[i][j].Status.PodIP
	//				if curIP == master.IP {
	//					masterfound = true
	//					continue
	//				}
	//				runningSlavesIPs.Insert(curIP)
	//			}
	//
	//			if masterfound {
	//				break
	//			}
	//		}
	//
	//		// delete the slaves those aren't in the set 'runningSlavesIps'
	//		for _, slave := range master.Slaves {
	//			if !runningSlavesIPs.Has(slave.IP) {
	//				if err = c.deleteNode(pods[0][0], master.IP+":6379", slave.ID); err != nil {
	//					return err
	//				}
	//				if err = c.clusterReset(pods[0][0], slave.IP); err != nil {
	//					return err
	//				}
	//			}
	//		}
	//	}
	//}
	if err = c.ensureExtraSlavesBeRemoved(pods); err != nil {
		return err
	}

	//=================================

	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//ip = pods[0][0].Status.PodIP
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//
	//// remove master(s)
	//if existingMasterCnt > c.Cluster.MasterCnt {
	//	var (
	//		masterIDsToBeRemoved, masterIDsToBeKept sets.String//[]string
	//		slotsPerMaster, slotsRequired           int //, allocatedSlotsCnt int
	//	)
	//
	//	slotsPerMaster = 16384 / c.Cluster.MasterCnt
	//	for i := 0; i < c.Cluster.MasterCnt; i++ {
	//		//for j := 0; j <= c.Cluster.Replicas; j++ {
	//			//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	//			//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	//			//curIP := pods[i][0].Status.PodIP
	//			if nodesConf, err = c.getClusterNodes(pods[i][0], pods[i][0].Status.PodIP); err != nil {
	//				return err
	//			}
	//			myConf := getMyConf(nodesConf)
	//			masterIDsToBeKept.Insert(getNodeId(myConf))
	//			//masterIDsToBeKept = append(masterIDsToBeKept, getNodeId(myConf))
	//			//
	//			//if getNodeRole(myConf) == "master" {
	//			//	if i < c.Cluster.MasterCnt {
	//			//		masterIDsToBeKept = append(masterIDsToBeKept, getNodeId(myConf))
	//			//	} else {
	//			//		masterIDsToBeRemoved = append(masterIDsToBeRemoved, getNodeId(myConf))
	//			//	}
	//			//} else if getNodeRole(myConf) == "slave" {
	//			//	if i >= c.Cluster.MasterCnt {
	//			//		deleteNode(ip+":6379", getNodeId(myConf))
	//			//		clusterReset(curIP)
	//			//	}
	//			//}
	//		//}
	//	}
	//	for masterID, _ := range nds {
	//		if !masterIDsToBeKept.Has(masterID) {
	//			masterIDsToBeRemoved.Insert(masterID)
	//		}
	//	}
	//
	//	for i, to := range masterIDsToBeKept.UnsortedList() {
	//		slotsRequired = slotsPerMaster
	//		if i == masterIDsToBeKept.Len()-1 {
	//			// this change is only for the last master that needs slots
	//			slotsRequired = 16384 - (slotsPerMaster * i)
	//		}
	//
	//		for _, from := range masterIDsToBeRemoved.UnsortedList() {
	//			// compare with slotsRequired
	//			if nds[to].SlotsCnt < slotsRequired {
	//				// But compare with slotsPerMaster. Existing masters always need slots equal to
	//				// slotsPerMaster not slotsRequired since slotsRequired may change for last master
	//				// that is being added.
	//				if nds[from].SlotsCnt > 0 {
	//					slots := nds[from].SlotsCnt
	//					if slots > slotsRequired-nds[to].SlotsCnt {
	//						slots = slotsRequired - nds[to].SlotsCnt
	//					}
	//
	//					if err = c.reshard(pods[0][0], nds, from, to, slots); err != nil {
	//						return err
	//					}
	//					nds[to].SlotsCnt += slots
	//					nds[from].SlotsCnt -= slots
	//				}
	//			} else {
	//				break
	//			}
	//		}
	//	}
	//
	//	for _, id := range masterIDsToBeRemoved.UnsortedList() {
	//		for _, slave := range nds[id].Slaves {
	//			if err = c.deleteNode(pods[0][0], nds[id].IP+":6379", slave.ID); err != nil {
	//				return err
	//			}
	//			if err = c.clusterReset(pods[0][0], slave.IP); err != nil {
	//				return err
	//			}
	//		}
	//		if err = c.deleteNode(pods[0][0], pods[0][0].Status.PodIP+":6379", id); err != nil {
	//			return err
	//		}
	//		if err = c.clusterReset(pods[0][0], nds[id].IP); err != nil {
	//			return err
	//		}
	//		//deleteNode(ip+":6379", masterIDToBeRemoved)
	//		//clusterReset(nds[masterIDToBeRemoved].IP)
	//	}
	//}
	if err = c.ensureExtraMastersBeRemoved(pods); err != nil {
		return err
	}

	//=================================

	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//ip = pods[0][0].Status.PodIP
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//
	//if existingMasterCnt > 1 {
	//	// add new master(s)
	//	if existingMasterCnt < c.Cluster.MasterCnt {
	//		for i := existingMasterCnt; i < c.Cluster.MasterCnt; i++ {
	//			//newMasterIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, 0, c.GoverningService, c.Namespace))
	//			//newMasterIp := ips[i][0]
	//			if err = c.addNode(pods[0][0], pods[i][0].Status.PodIP+":6379", ip+":6379", ""); err != nil {
	//				return err
	//			}
	//		}
	//		nds = processNodesConf(getClusterNodes(ip))
	//	}
	//
	//	// add slots to empty master(s)
	//	var (
	//		masterIDsWithLessSlots, masterIDsWithExtraSlots []string
	//		//nonEmptyMastersId, emptyMastersId                []string
	//		slotsPerMaster, slotsRequired int //, allocatedSlotsCnt int
	//	)
	//
	//	slotsPerMaster = 16384 / c.Cluster.MasterCnt
	//	for masterID, master := range nds {
	//		if master.SlotsCnt < slotsPerMaster {
	//			masterIDsWithLessSlots = append(masterIDsWithLessSlots, masterID)
	//			//emptyMastersId = append(emptyMastersId, masterId)
	//		} else {
	//			masterIDsWithExtraSlots = append(masterIDsWithExtraSlots, masterID)
	//			//nonEmptyMastersId = append(nonEmptyMastersId, masterId)
	//		}
	//	}
	//
	//	//===================================
	//
	//	for i, to := range masterIDsWithLessSlots {
	//		slotsRequired = slotsPerMaster
	//		if i == len(masterIDsWithLessSlots)-1 {
	//			// this change is only for the last master that needs slots
	//			slotsRequired = 16384 - (slotsPerMaster * i)
	//		}
	//
	//		//allocatedSlotsCnt = nds[to].SlotsCnt
	//		for _, from := range masterIDsWithExtraSlots {
	//			// compare with slotsRequired
	//			if nds[to].SlotsCnt < slotsRequired {
	//				// But compare with slotsPerMaster. Existing masters always need slots equal to
	//				// slotsPerMaster not slotsRequired since slotsRequired may change for last master
	//				// that is being added.
	//				if nds[from].SlotsCnt > slotsPerMaster {
	//					slots := nds[from].SlotsCnt - slotsPerMaster
	//					if slots > slotsRequired-nds[to].SlotsCnt {
	//						slots = slotsRequired - nds[to].SlotsCnt
	//					}
	//
	//					reshard(nds, from, to, slots)
	//					nds[to].SlotsCnt += slots
	//					nds[from].SlotsCnt -= slots
	//				}
	//			} else {
	//				break
	//			}
	//		}
	//	}
	//
	//	//===================================
	//
	if err = c.ensureNewMastersBeAdded(pods); err != nil {
		return err
	}
	if err = c.rebalanceSlots(pods); err != nil {
		return err
	}

	////==================================
	//
	////for i, emptyMasterId := range emptyMastersId {
	////	slotsRequired = slotsPerMaster
	////	if i == len(emptyMastersId)-1 {
	////		// this change is only for last master that is being added
	////		slotsRequired = 16384 - (slotsPerMaster * i)
	////	}
	////
	////	allocatedSlotsCnt = nds[emptyMasterId].SlotsCnt
	////	for _, masterId := range nonEmptyMastersId {
	////		// compare with slotsRequired
	////		if allocatedSlotsCnt < slotsRequired {
	////			// But compare with slotsPerMaster. Existing masters always need slots equal to
	////			// slotsPerMaster not slotsRequired since slotsRequired may change for last master
	////			// that is being added.
	////			if nds[masterId].SlotsCnt > slotsPerMaster {
	////				slots := nds[masterId].SlotsCnt - slotsPerMaster
	////				if slots > slotsRequired - allocatedSlotsCnt {
	////					slots = slotsRequired - allocatedSlotsCnt
	////				}
	////
	////				reshard(masterId, emptyMasterId, strconv.Itoa(slots))
	////				allocatedSlotsCnt += slots
	////				nds[masterId].SlotsCnt -= slots
	////			}
	////		} else {
	////			break
	////		}
	////	}
	////}
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//ip = pods[0][0].Status.PodIP
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//nds = processNodesConf(nodesConf)
	//existingMasterCnt = len(nds)
	//
	//// add new slave(s)
	//for masterID, master := range nds {
	//	if len(master.Slaves) < c.Cluster.Replicas {
	//		for i := 0; i < c.Cluster.MasterCnt; i++ {
	//			masterFound := false
	//			curSlaves := sets.NewString()
	//			for j := 0; j <= c.Cluster.Replicas; j++ {
	//				if ips[i][j] == master.IP {
	//					masterFound = true
	//					for _, slave := range master.Slaves {
	//						curSlaves.Insert(slave.IP)
	//					}
	//					break
	//				}
	//				//slaves.Insert(ips[i][j])
	//			}
	//
	//			if masterFound {
	//				for j := 0; j <= c.Cluster.Replicas; j++ {
	//					if ips[i][j] != master.IP && !curSlaves.Has(ips[i][j]) {
	//						addNode(ips[i][j]+":6379", master.IP+":6379", masterID)
	//					}
	//				}
	//				break
	//			}
	//		}
	//	}
	//}
	////for i := 0; i < c.Cluster.MasterCnt; i++ {
	////	curMasterID := ""
	////	curMasterIP := ""
	////FindMaster:
	////	for j := 0; j <= c.Cluster.Replicas; j++ {
	////		//curIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	////		//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	////		//curIP := ips[i][j]
	////		for _, master := range nds {
	////			if master.IP == ips[i][j] {
	////				curMasterIP = ips[i][j]
	////				curMasterID = master.ID
	////				break FindMaster
	////			}
	////		}
	////	}
	////
	////	for j := 0; j <= c.Cluster.Replicas; j++ {
	////		//curIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	////		//	c.BaseName, i, j, c.GoverningService, c.Namespace))
	////		//curIP := ips[i][j]
	////		exists := false
	////	FindSlave:
	////		for _, slave := range nds[curMasterID].Slaves {
	////			if slave.IP == ips[i][j] {
	////				exists = true
	////				break FindSlave
	////			}
	////		}
	////
	////		if !exists {
	////			addNode(ips[i][j]+":6379", curMasterIP+":6379", curMasterID)
	////		}
	////	}
	////}
	if err = c.ensureNewSlavesBeAdded(pods); err != nil {
		return err
	}

	return nil
}

func (c Config) ensureCluster(pods [][]*core.Pod) error {
	log.Infoln("\n\nensuring new cluster...")

	var (
		masterAddrs   []string
		masterNodeIds []string
		//pods          [][]*core.Pod
		err       error
		nodesConf string
		nodes     [][]RedisNode
	)
	masterAddrs = make([]string, c.Cluster.MasterCnt)
	masterNodeIds = make([]string, c.Cluster.MasterCnt)
	// ======================
	nodes, err = c.getOrderedNodes(pods)
	if err != nil {
		return err
	}
	if len(nodes) > 1 {
		return nil
	}
	for i := 0; i < c.Cluster.MasterCnt; i++ {
		masterAddrs[i] = pods[i][0].Status.PodIP + ":6379"
		if nodesConf, err = c.getClusterNodes(pods[0][0], pods[i][0].Status.PodIP); err != nil {
			return err
		}
		masterNodeIds[i] = getNodeId(getMyConf(nodesConf))
	}
	if err = c.createCluster(pods[0][0], masterAddrs...); err != nil {
		return err
	}
	time.Sleep(time.Second * 15)

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		for j := 1; j <= c.Cluster.Replicas; j++ {
			if err = c.addNode(
				pods[0][0],
				pods[i][j].Status.PodIP+":6379", masterAddrs[i], masterNodeIds[i]); err != nil {
				return err
			}
		}
	}
	time.Sleep(time.Second * 15)
	// ==================================
	//
	//if pods, err = c.getInstances(); err != nil {
	//	return err
	//}
	//if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
	//	return err
	//}
	//
	//if strings.Count(nodesConf, "master") > 1 {
	//	return nil
	//}
	//
	//for i := 0; i < c.Cluster.MasterCnt; i++ {
	//	//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
	//	//	c.BaseName, i, 0, c.GoverningService, c.Namespace))
	//	//ip := ips[i][0]
	//	masterAddrs[i] = pods[i][0].Status.PodIP + ":6379"
	//	if nodesConf, err = c.getClusterNodes(pods[0][0], pods[i][0].Status.PodIP); err != nil {
	//		return err
	//	}
	//	masterNodeIds[i] = getNodeId(getMyConf(nodesConf))
	//}
	//if err = c.createCluster(pods[0][0], masterAddrs...); err != nil {
	//	return err
	//}
	//
	//for i := 0; i < c.Cluster.MasterCnt; i++ {
	//	for j := 1; j <= c.Cluster.Replicas; j++ {
	//		//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, j, c.GoverningService, c.Namespace))
	//		//ip := ips[i][j]
	//		if err = c.addNode(
	//			pods[0][0],
	//			pods[i][j].Status.PodIP+":6379", masterAddrs[i], masterNodeIds[i]); err != nil {
	//			return err
	//		}
	//	}
	//}

	return nil
}

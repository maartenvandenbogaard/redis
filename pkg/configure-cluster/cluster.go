package configure_cluster

import (
	"strconv"
	"strings"

	"github.com/appscode/go/log"
	//kerr "k8s.io/apimachinery/pkg/api/errors"
	"time"

	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/redis/pkg/exec"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	RestConfig *rest.Config
	KubeClient kubernetes.Interface

	BaseName  string
	Namespace string

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

	if err := config.waitUntilRedisServersToBeReady(pods); err != nil {
		return err
	}
	if err := config.configureClusterState(pods); err != nil {
		return err
	}

	return nil
}

func (c Config) createCluster(pod *core.Pod, addrs ...string) error {
	e := exec.NewExecWithInputOptions(c.RestConfig, c.KubeClient, "yes")
	_, err := e.Run(pod, ClusterCreateCmd(0, addrs...)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to create cluster using (%v)", addrs)
	}

	return nil
}

func (c Config) addNode(pod *core.Pod, newAddr, existingAddr, masterId string) error {
	var err error

	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	if masterId == "" {
		if _, err = e.Run(pod, AddNodeAsMasterCmd(newAddr, existingAddr)...); err != nil {
			return errors.Wrapf(err, "Failed to add %q as a master", newAddr)
		}
	} else {
		if _, err = e.Run(pod, AddNodeAsSlaveCmd(newAddr, existingAddr, masterId)...); err != nil {
			return errors.Wrapf(err, "Failed to add %q as a slave of master with id %q", newAddr, masterId)
		}
	}

	return nil
}

func (c Config) deleteNode(pod *core.Pod, existingAddr, deletingNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, DeleteNodeCmd(existingAddr, deletingNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to delete node with ID %q", deletingNodeID)
	}

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
	_, err := e.Run(pod, ClusterResetCmd(ip)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to reset node %q", ip)
	}

	return nil
}

func (c Config) clusterFailover(pod *core.Pod, ip string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterFailoverCmd(ip)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to failover node %q", ip)
	}

	return nil
}

func (c Config) clusterSetSlotImporting(pod *core.Pod, dstNodeIP, slot, srcNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotImportingCmd(dstNodeIP, slot, srcNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in destination node %q as 'importing' from source node with ID %q",
			slot, dstNodeIP, srcNodeID)
	}

	return nil
}

func (c Config) clusterSetSlotMigrating(pod *core.Pod, srcNodeIP, slot, dstNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotMigratingCmd(srcNodeIP, slot, dstNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in source node %q as 'migrating' to destination node with ID %q",
			slot, srcNodeIP, dstNodeID)
	}

	return nil
}

func (c Config) clusterSetSlotNode(pod *core.Pod, toNodeIP, slot, dstNodeID string) error {
	e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
	_, err := e.Run(pod, ClusterSetSlotNodeCmd(toNodeIP, slot, dstNodeID)...)
	if err != nil {
		return errors.Wrapf(err, "Failed to set slot %q in node %q as 'node' to destination node with ID %q",
			slot, toNodeIP, dstNodeID)
	}

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

func (c Config) reshard(pod *core.Pod, nodes [][]RedisNode, src, dst, requstedSlotsCount int) error {
	log.Infof("Resharding %d slots from %q to %q...", requstedSlotsCount, nodes[src][0].IP, nodes[dst][0].IP)

	var (
		movedSlotsCount int
		err             error
	)

	movedSlotsCount = 0
	need := requstedSlotsCount

	//Reshard:
	for i := range nodes[src][0].SlotStart {
		if movedSlotsCount >= requstedSlotsCount {
			//break Reshard
			break
		}

		start := nodes[src][0].SlotStart[i]
		end := nodes[src][0].SlotEnd[i]
		if end-start+1 > need {
			end = start + need - 1
		}
		cmd := []string{"/conf/cluster.sh", "reshard", nodes[src][0].IP, nodes[src][0].ID, nodes[dst][0].IP, nodes[dst][0].ID,
			strconv.Itoa(start), strconv.Itoa(end),
		}

		e := exec.NewExecWithDefaultOptions(c.RestConfig, c.KubeClient)
		_, err = e.Run(pod, cmd...)
		if err != nil {
			return errors.Wrapf(err, "Failed to reshard %d slots from %q to %q",
				requstedSlotsCount, nodes[src][0].IP, nodes[dst][0].IP)
		}

		movedSlotsCount += (end - start + 1)
		need -= (end - start + 1)
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

func (c Config) waitUntilRedisServersToBeReady(pods [][]*core.Pod) error {
	var err error

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		for j := 0; j <= c.Cluster.Replicas; j++ {
			if err = wait.PollImmediate(time.Second, time.Minute*5, func() (bool, error) {
				if pong, _ := c.ping(pods[i][j], pods[i][j].Status.PodIP); pong == "PONG" {
					return true, nil
				}

				return false, nil
			}); err != nil {
				return errors.Wrapf(err, "%q is not ready yet", pods[i][j].Status.PodIP)
			}
		}
	}
	log.Infoln("All redis servers are ready")

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

	return nds
}

func (c Config) ensureFirstPodAsMaster(pods [][]*core.Pod) error {
	log.Infoln("Ensuring 1st pod as master in each statefulSet...")

	var (
		err       error
		nodesConf string
	)

	if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
		return err
	}

	if strings.Count(nodesConf, "master") > 1 {
		for i := 0; i < c.Cluster.MasterCnt; i++ {
			if nodesConf, err = c.getClusterNodes(pods[i][0], pods[i][0].Status.PodIP); err != nil {
				return err
			}
			if getNodeRole(getMyConf(nodesConf)) != "master" {
				if err = c.clusterFailover(pods[i][0], pods[i][0].Status.PodIP); err != nil {
					return err
				}
				time.Sleep(time.Second * 5)
			}
		}
	}

	return nil
}

func (c Config) getOrderedNodes(pods [][]*core.Pod) ([][]RedisNode, error) {
	var (
		err          error
		nodesConf    string
		nodes        map[string]*RedisNode
		orderedNodes [][]RedisNode
	)

	if err = c.ensureFirstPodAsMaster(pods); err != nil {
		return nil, err
	}

Again:
	for {
		if nodesConf, err = c.getClusterNodes(pods[0][0], pods[0][0].Status.PodIP); err != nil {
			return nil, err
		}
		nodes = processNodesConf(nodesConf)
		for _, master := range nodes {
			for i := 0; i < len(pods); i++ {
				if pods[i][0].Status.PodIP == master.IP {
					for _, slave := range master.Slaves {
						for k := 0; k < len(pods); k++ {
							for j := 1; j < len(pods[k]); j++ {
								if pods[k][j].Status.PodIP == slave.IP && i != k {
									if err = c.clusterReplicate(
										pods[k][j], pods[k][j].Status.PodIP,
										getNodeId(getNodeConfByIP(nodesConf, pods[k][0].Status.PodIP))); err != nil {
										return nil, err
									}
									time.Sleep(time.Second * 5)
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

	orderedNodes = make([][]RedisNode, len(nodes))
	for i := 0; i < len(nodes); i++ {
		for _, master := range nodes {
			if master.IP == pods[i][0].Status.PodIP {
				orderedNodes[i] = make([]RedisNode, len(master.Slaves)+1)
				orderedNodes[i][0] = *master
				for j := 1; j < len(orderedNodes[i]); j++ {
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

	//if len(orderedNodes) > 1 {
	//	for i := range orderedNodes {
	//		if len(orderedNodes[i]) == 0 {
	//			continue
	//		}
	//
	//		fmt.Println(">>>>>>>> index =", i)
	//		fmt.Println("=============================================================")
	//		fmt.Println("{")
	//		fmt.Println("\t ID:", orderedNodes[i][0].ID)
	//		fmt.Println("\t IP:", orderedNodes[i][0].IP)
	//		fmt.Println("\t Role:", orderedNodes[i][0].Role)
	//		fmt.Println("\t Down:", orderedNodes[i][0].Down)
	//		fmt.Println("\t Slot Count:", orderedNodes[i][0].SlotsCnt)
	//		fmt.Println("\t Slot Start:", orderedNodes[i][0].SlotStart)
	//		fmt.Println("\t Slot End:", orderedNodes[i][0].SlotEnd)
	//		for j := 1; j < len(orderedNodes[i]); j++ {
	//			fmt.Println("\t{")
	//			fmt.Println("\t\t ID:", orderedNodes[i][j].ID)
	//			fmt.Println("\t\t IP:", orderedNodes[i][j].IP)
	//			fmt.Println("\t\t Role:", orderedNodes[i][j].Role)
	//			fmt.Println("\t\t Down:", orderedNodes[i][j].Down)
	//			fmt.Println("\t\t MasterId =", orderedNodes[i][j].Master.ID)
	//			fmt.Println("\t}")
	//		}
	//		fmt.Println("}")
	//	}
	//}

	return orderedNodes, nil
}

func (c Config) ensureExtraSlavesBeRemoved(pods [][]*core.Pod) error {
	log.Infoln("Ensuring extra slaves be removed...")

	var (
		err   error
		nodes [][]RedisNode
	)

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

	return nil
}

func (c Config) ensureExtraMastersBeRemoved(pods [][]*core.Pod) error {
	log.Infoln("Ensuring extra masters be removed...")

	var (
		err                           error
		existingMasterCnt             int
		nodes                         [][]RedisNode
		slotsPerMaster, slotsRequired int
	)

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
			// todo: need to update this logic for using '-x' option in cluster.sh
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

	return nil
}

func (c Config) ensureNewMastersBeAdded(pods [][]*core.Pod) error {
	log.Infoln("Ensuring new masters be added...")

	var (
		err               error
		existingMasterCnt int
		nodes             [][]RedisNode
	)

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
		}
	}

	return nil
}

func (c Config) rebalanceSlots(pods [][]*core.Pod) error {
	log.Infoln("Ensuring slots are rebalanced...")

	var (
		err                                                     error
		existingMasterCnt                                       int
		nodes                                                   [][]RedisNode
		masterIndicesWithLessSlots, masterIndicesWithExtraSlots []int
		slotsPerMaster, slotsRequired                           int
	)

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
	}

	return nil
}

func (c Config) ensureNewSlavesBeAdded(pods [][]*core.Pod) error {
	log.Infoln("Ensuring new slaves be added...")

	var (
		err               error
		existingMasterCnt int
		nodes             [][]RedisNode
	)

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
	}

	return nil
}

func (c Config) configureClusterState(pods [][]*core.Pod) error {
	var err error

	if err = c.ensureCluster(pods); err != nil {
		return err
	}

	if err = c.ensureExtraSlavesBeRemoved(pods); err != nil {
		return err
	}

	if err = c.ensureExtraMastersBeRemoved(pods); err != nil {
		return err
	}

	if err = c.ensureNewMastersBeAdded(pods); err != nil {
		return err
	}
	if err = c.rebalanceSlots(pods); err != nil {
		return err
	}

	if err = c.ensureNewSlavesBeAdded(pods); err != nil {
		return err
	}

	return nil
}

func (c Config) ensureCluster(pods [][]*core.Pod) error {
	log.Infoln("Ensuring new cluster...")

	var (
		masterAddrs   []string
		masterNodeIds []string
		err           error
		nodesConf     string
		nodes         [][]RedisNode
	)
	masterAddrs = make([]string, c.Cluster.MasterCnt)
	masterNodeIds = make([]string, c.Cluster.MasterCnt)

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

	return nil
}

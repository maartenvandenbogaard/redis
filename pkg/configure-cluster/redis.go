package configure_cluster

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/appscode/go/log"
	"github.com/appscode/go/sets"
	"github.com/tamalsaha/go-oneliners"

	//kerr "k8s.io/apimachinery/pkg/api/errors"
	"time"

	core_util "github.com/appscode/kutil/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ConfigureRedisCluster() {
	config := getConfig()
	//_ = getConfigFromEnv()
	//clusterGetKeysInSlot("172.17.0.5", "3168")
	//fmt.Printf("\n>%s<\n", getClusterNodes("172.17.0.5"))
	//fmt.Printf("\n>%s<\n", getClusterNodes("172.17.0.3"))
	//fmt.Printf("\n>%s<\n", getClusterNodes("172.17.0.14"))
	//fmt.Printf("\n>%s<\n", getClusterNodes("172.17.0.6"))
	//config.getIPs()
	config.waitUntillRedisServersToBeReady()
	config.configureClusterState()

	select {}
}

func (c Config) getIPs() [][]string {
	//ips := make(map[string]string)
	ips := make([][]string, c.Cluster.MasterCnt)
	for i := 0; i < c.Cluster.MasterCnt; i++ {
		ips[i] = []string{}

		//err := core_util.WaitUntilPodRunningBySelector(
		//	c.KubeClient,
		//	c.Namespace,
		//	&metav1.LabelSelector{
		//		MatchLabels: map[string]string{
		//			api.LabelDatabaseName: c.BaseName,
		//			api.LabelDatabaseKind: api.ResourceKindRedis,
		//		},
		//	},
		//	c.Cluster.Replicas,
		//)
		//if err != nil {
		//	panic(err)
		//}

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

			err := core_util.WaitUntilPodRunning(
				c.KubeClient,
				metav1.ObjectMeta{
					Name:      podName,
					Namespace: c.Namespace,
				},
			)
			if err != nil {
				panic(err)
			}
			log.Infof("%s/%s is ready", c.Namespace, podName)

			pod, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
			if err != nil {
				panic(err)
			}
			ips[i] = append(ips[i], pod.Status.PodIP)

			//pod, err := c.KubeClient.CoreV1().Pods(c.Namespace).Get(podName, metav1.GetOptions{})
			//if err != nil {
			//	if !kerr.IsNotFound(err) {
			//		panic(err)
			//	}
			//}
		}
	}

	oneliners.PrettyJson(ips)
	return ips

	//if pods, err := c.KubeClient.CoreV1().Pods(c.Namespace).List(metav1.ListOptions{
	//	LabelSelector: labels.Set{
	//		api.LabelDatabaseKind: api.ResourceKindRedis,
	//		api.LabelDatabaseName: c.BaseName,
	//	}.String(),
	//}); err == nil {
	//	for _, po := range pods.Items {
	//		ips[po.Name] = po.Status.PodIP
	//	}
	//
	//	oneliners.PrettyJson(ips, "ips")
	//} else {
	//	panic(err)
	//}
}

func getIPByHostName(host string) string {
	cmd := NewCmdWithDefaultOptions()
	var (
		out string
		err error
	)
	for {

		out, err = cmd.Run("ping", []string{"-c", "1", host}...)
		//oneliners.PrettyJson(host, "host")
		//oneliners.PrettyJson(out, "ip")
		//oneliners.PrettyJson(err.Error(), "ip err")

		if err == nil {
			break
		}
	}
	if err != nil {
		panic(err)
	}
	r, _ := regexp.Compile("([0-9]+).([0-9]+).([0-9]+).([0-9]+)")

	return r.FindString(out)
}

func createCluster(addrs ...string) {
	cmd := NewCmdWithInputOptions("yes")
	args := []string{"create"}
	args = append(args, addrs...)
	out, err := cmd.Run("redis-trib", args...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func addNode(newAddr, existingAddr, masterId string) {
	cmd := NewCmdWithDefaultOptions()
	var (
		out string
		err error
	)

	if masterId == "" {
		out, err = cmd.Run("redis-trib",
			[]string{"add-node", newAddr, existingAddr}...)
	} else {
		out, err = cmd.Run("redis-trib",
			[]string{"add-node", "--slave", "--master-id", masterId, newAddr, existingAddr}...)
	}

	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func deleteNode(existingAddr, nodeId string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-trib",
		[]string{"del-node", existingAddr, nodeId}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func ping(ip string) string {
	cmd := NewCmdWithDefaultOptions()
	pong, err := cmd.Run("redis-cli", []string{"-h", ip, "ping"}...)
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(pong)
}

func migrateKey(srcNodeIP, dstNodeIP, dstNodePort, key, dbID, timeout string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli",
		[]string{"-h", srcNodeIP, "migrate", dstNodeIP, dstNodePort, key, dbID, timeout}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func getClusterNodes(ip string) string {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli", []string{"-c", "-h", ip, "cluster", "nodes"}...)
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(out)
}

func clusterReset(ip string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli", []string{"-c", "-h", ip, "cluster", "reset"}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func clusterFailover(ip string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli", []string{"-c", "-h", ip, "cluster", "failover"}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func clusterSetSlotImporting(dstNodeIp, slot, srcNodeId string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli",
		[]string{"-c", "-h", dstNodeIp, "cluster", "setslot", slot, "importing", srcNodeId}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func clusterSetSlotMigrating(srcNodeIp, slot, dstNodeId string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli",
		[]string{"-c", "-h", srcNodeIp, "cluster", "setslot", slot, "migrating", dstNodeId}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func clusterSetSlotNode(toNodeIp, slot, dstNodeId string) {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli",
		[]string{"-c", "-h", toNodeIp, "cluster", "setslot", slot, "node", dstNodeId}...)
	if err != nil {
		panic(err)
	}

	fmt.Println(out)
}

func clusterGetKeysInSlot(srcNodeIp, slot string) string {
	cmd := NewCmdWithDefaultOptions()
	out, err := cmd.Run("redis-cli",
		[]string{"-c", "-h", srcNodeIp, "cluster", "getkeysinslot", slot, "1"}...)
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(out)
}

func reshard(nds map[string]*RedisNode, srcNodeID, dstNodeID string, requstedSlotsCount int) {
	//cmd := NewCmdWithDefaultOptions()
	//out, err := cmd.Run("redis-trib",
	//	[]string{"reshard", "--from", from, "--to", to, "--slots", slots, "--yes"}...)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Println(out)
	//
	var movedSlotsCount int
	movedSlotsCount = 0
Reshard:
	for i := range nds[srcNodeID].SlotStart {
		if movedSlotsCount >= requstedSlotsCount {
			break Reshard
		}

		start := nds[srcNodeID].SlotStart[i]
		end := nds[srcNodeID].SlotEnd[i]
		for slot := start; slot <= end; slot++ {
			if movedSlotsCount >= requstedSlotsCount {
				break Reshard
			}

			clusterSetSlotImporting(nds[dstNodeID].Ip, strconv.Itoa(slot), srcNodeID)
			clusterSetSlotMigrating(nds[srcNodeID].Ip, strconv.Itoa(slot), dstNodeID)
			for {
				key := clusterGetKeysInSlot(nds[srcNodeID].Ip, strconv.Itoa(slot))
				if key == "" {
					break
				}
				migrateKey(nds[srcNodeID].Ip, nds[dstNodeID].Ip, strconv.Itoa(nds[dstNodeID].Port), key, "0", "5000")
			}
			clusterSetSlotNode(nds[srcNodeID].Ip, strconv.Itoa(slot), dstNodeID)
			clusterSetSlotNode(nds[dstNodeID].Ip, strconv.Itoa(slot), dstNodeID)

			for masterId, master := range nds {
				if masterId != srcNodeID && masterId != dstNodeID {
					clusterSetSlotNode(master.Ip, strconv.Itoa(slot), dstNodeID)
				}
			}
			movedSlotsCount++
		}
	}

}

func getMyConf(nodesConf string) (myConf string) {
	nodes := strings.Split(nodesConf, "\n")
	for _, node := range nodes {
		if strings.Contains(node, "myself") {
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

func getMasterID(nodeConf string) (masterRole string) {
	masterRole = ""
	if getNodeRole(nodeConf) == "slave" {
		masterRole = strings.Split(nodeConf, " ")[3]
	}

	return masterRole
}

func (c Config) waitUntillRedisServersToBeReady() {
	ips := c.getIPs()
	for i := 0; i < c.Cluster.MasterCnt; i++ {
		for j := 0; j <= c.Cluster.Replicas; j++ {
			//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, j, c.GoverningService, c.Namespace))
			for {
				if ping(ips[i][j]) == "PONG" {
					log.Infof("%s is ready", ips[i][j])
					break
				}
			}
		}
	}
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

		if strings.Contains(parts[2], "master") {
			nd := RedisNode{
				Id:   parts[0],
				Ip:   strings.Split(parts[1], ":")[0],
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

			nds[nd.Id] = &nd
		}
	}

	for _, node := range nodes {
		node = strings.TrimSpace(node)
		parts := strings.Split(strings.TrimSpace(node), " ")

		if strings.Contains(parts[2], "slave") {
			nd := RedisNode{
				Id:   parts[0],
				Ip:   strings.Split(parts[1], ":")[0],
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

	for masterId, master := range nds {
		fmt.Println(">>>>>>>> masterId =", masterId)
		fmt.Println("=============================================================")
		fmt.Println("{")
		fmt.Println("\t ID:", masterId)
		fmt.Println("\t IP:", master.Ip)
		fmt.Println("\t Role:", master.Role)
		fmt.Println("\t Down:", master.Down)
		fmt.Println("\t Slot Count:", master.SlotsCnt)
		fmt.Println("\t Slot Start:", master.SlotStart)
		fmt.Println("\t Slot End:", master.SlotEnd)
		for _, slave := range master.Slaves {
			fmt.Println("\t{")
			fmt.Println("\t\t ID:", slave.Id)
			fmt.Println("\t\t IP:", slave.Ip)
			fmt.Println("\t\t Role:", slave.Role)
			fmt.Println("\t\t Down:", slave.Down)
			fmt.Println("\t\t MasterId =", slave.Master.Id)
			fmt.Println("\t}")
		}
		fmt.Println("}")
	}

	return nds
}

func (c Config) ensureFirstPodAsMaster() {
	ips := c.getIPs()
	//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, 0, 0, c.GoverningService, c.Namespace))
	ip := ips[0][0]
	nodesConf := getClusterNodes(ip)

	if strings.Count(nodesConf, "master") > 1 {
		for i := 0; i < c.Cluster.MasterCnt; i++ {
			nodeConf := getMyConf(getClusterNodes(ips[i][0]))
			if getNodeRole(nodeConf) != "master" {
				clusterFailover(ips[i][0])
			}
		}
		log.Infoln("Waiting to update cluster for ensuring 1st pod as master")
		time.Sleep(time.Minute)
	}
}

func (c Config) configureClusterState() {
	c.ensureFirstPodAsMaster()

	ips := c.getIPs()
	//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, 0, 0, c.GoverningService, c.Namespace))
	ip := ips[0][0]
	nodesConf := getClusterNodes(ip)
	nds := processNodesConf(nodesConf)
	masterCnt := len(nds)
	// remove slave(s)
	for _, master := range nds {
		if c.Cluster.Replicas < len(master.Slaves) {
			var (
				runningSlavesIPs sets.String
				masterfound      bool
			)

			// find slaves' ips of this master those need to be keep alive
			for i := 0; i < c.Cluster.MasterCnt; i++ {
				runningSlavesIPs = sets.NewString()
				masterfound = false

				for j := 0; j <= c.Cluster.Replicas; j++ {
					//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
					//	c.BaseName, i, j, c.GoverningService, c.Namespace))
					curIP := ips[i][j]
					if curIP == master.Ip {
						masterfound = true
						continue
					}
					runningSlavesIPs.Insert(curIP)
				}

				if masterfound {
					break
				}
			}

			// delete the slaves those aren't in the set 'runningSlavesIps'
			for _, slave := range master.Slaves {
				if !runningSlavesIPs.Has(slave.Ip) {
					deleteNode(master.Ip+":6379", slave.Id)
					clusterReset(slave.Ip)
				}
			}
		}
	}

	ips = c.getIPs()
	ip = ips[0][0]
	nodesConf = getClusterNodes(ip)
	nds = processNodesConf(nodesConf)
	masterCnt = len(nds)

	// remove master(s)
	if masterCnt > c.Cluster.MasterCnt {
		var (
			masterIDsToBeRemoved, masterIDsToBeKept []string
			slotsPerMaster, slotsRequired           int //, allocatedSlotsCnt int
		)

		slotsPerMaster = 16384 / c.Cluster.MasterCnt
		for i := 0; i < masterCnt; i++ {
			for j := 0; j <= c.Cluster.Replicas; j++ {
				//curIP := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
				//	c.BaseName, i, j, c.GoverningService, c.Namespace))
				curIP := ips[i][j]
				myConf := getMyConf(getClusterNodes(curIP))

				if getNodeRole(myConf) == "master" {
					if i < c.Cluster.MasterCnt {
						masterIDsToBeKept = append(masterIDsToBeKept, getNodeId(myConf))
					} else {
						masterIDsToBeRemoved = append(masterIDsToBeRemoved, getNodeId(myConf))
					}
				} else if getNodeRole(myConf) == "slave" {
					if i >= c.Cluster.MasterCnt {
						deleteNode(ip+":6379", getNodeId(myConf))
						clusterReset(curIP)
					}
				}
			}
		}

		for i, to := range masterIDsToBeKept {
			slotsRequired = slotsPerMaster
			if i == len(masterIDsToBeKept)-1 {
				// this change is only for the last master that needs slots
				slotsRequired = 16384 - (slotsPerMaster * i)
			}

			for _, from := range masterIDsToBeRemoved {
				// compare with slotsRequired
				if nds[to].SlotsCnt < slotsRequired {
					// But compare with slotsPerMaster. Existing masters always need slots equal to
					// slotsPerMaster not slotsRequired since slotsRequired may change for last master
					// that is being added.
					if nds[from].SlotsCnt > 0 {
						slots := nds[from].SlotsCnt
						if slots > slotsRequired-nds[to].SlotsCnt {
							slots = slotsRequired - nds[to].SlotsCnt
						}

						reshard(nds, from, to, slots)
						nds[to].SlotsCnt += slots
						nds[from].SlotsCnt -= slots
					}
				} else {
					break
				}
			}
		}

		for _, masterIDToBeRemoved := range masterIDsToBeRemoved {
			deleteNode(ip+":6379", masterIDToBeRemoved)
			clusterReset(nds[masterIDToBeRemoved].Ip)
		}
	}

	ips = c.getIPs()
	ip = ips[0][0]
	nodesConf = getClusterNodes(ip)
	nds = processNodesConf(nodesConf)
	masterCnt = len(nds)

	if masterCnt > 1 {
		// add new master(s)
		if masterCnt < c.Cluster.MasterCnt {
			for i := masterCnt; i < c.Cluster.MasterCnt; i++ {
				//newMasterIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, 0, c.GoverningService, c.Namespace))
				newMasterIp := ips[i][0]
				addNode(newMasterIp+":6379", ip+":6379", "")
			}
			nds = processNodesConf(getClusterNodes(ip))
		}

		// add slots to empty master(s)
		var (
			masterIDsWithLessSlots, masterIDsWithExtraSlots []string
			//nonEmptyMastersId, emptyMastersId                []string
			slotsPerMaster, slotsRequired int //, allocatedSlotsCnt int
		)

		slotsPerMaster = 16384 / c.Cluster.MasterCnt
		for masterID, master := range nds {
			if master.SlotsCnt < slotsPerMaster {
				masterIDsWithLessSlots = append(masterIDsWithLessSlots, masterID)
				//emptyMastersId = append(emptyMastersId, masterId)
			} else {
				masterIDsWithExtraSlots = append(masterIDsWithExtraSlots, masterID)
				//nonEmptyMastersId = append(nonEmptyMastersId, masterId)
			}
		}

		// ===================================
		for i, to := range masterIDsWithLessSlots {
			slotsRequired = slotsPerMaster
			if i == len(masterIDsWithLessSlots)-1 {
				// this change is only for the last master that needs slots
				slotsRequired = 16384 - (slotsPerMaster * i)
			}

			//allocatedSlotsCnt = nds[to].SlotsCnt
			for _, from := range masterIDsWithExtraSlots {
				// compare with slotsRequired
				if nds[to].SlotsCnt < slotsRequired {
					// But compare with slotsPerMaster. Existing masters always need slots equal to
					// slotsPerMaster not slotsRequired since slotsRequired may change for last master
					// that is being added.
					if nds[from].SlotsCnt > slotsPerMaster {
						slots := nds[from].SlotsCnt - slotsPerMaster
						if slots > slotsRequired-nds[to].SlotsCnt {
							slots = slotsRequired - nds[to].SlotsCnt
						}

						reshard(nds, from, to, slots)
						nds[to].SlotsCnt += slots
						nds[from].SlotsCnt -= slots
					}
				} else {
					break
				}
			}
		}
		// ===================================

		//for i, emptyMasterId := range emptyMastersId {
		//	slotsRequired = slotsPerMaster
		//	if i == len(emptyMastersId)-1 {
		//		// this change is only for last master that is being added
		//		slotsRequired = 16384 - (slotsPerMaster * i)
		//	}
		//
		//	allocatedSlotsCnt = nds[emptyMasterId].SlotsCnt
		//	for _, masterId := range nonEmptyMastersId {
		//		// compare with slotsRequired
		//		if allocatedSlotsCnt < slotsRequired {
		//			// But compare with slotsPerMaster. Existing masters always need slots equal to
		//			// slotsPerMaster not slotsRequired since slotsRequired may change for last master
		//			// that is being added.
		//			if nds[masterId].SlotsCnt > slotsPerMaster {
		//				slots := nds[masterId].SlotsCnt - slotsPerMaster
		//				if slots > slotsRequired - allocatedSlotsCnt {
		//					slots = slotsRequired - allocatedSlotsCnt
		//				}
		//
		//				reshard(masterId, emptyMasterId, strconv.Itoa(slots))
		//				allocatedSlotsCnt += slots
		//				nds[masterId].SlotsCnt -= slots
		//			}
		//		} else {
		//			break
		//		}
		//	}
		//}

		ips = c.getIPs()
		ip = ips[0][0]
		nodesConf = getClusterNodes(ip)
		nds = processNodesConf(nodesConf)
		masterCnt = len(nds)

		// add new slave(s)
		for masterID, master := range nds {
			if len(master.Slaves) < c.Cluster.Replicas {
				for i := 0; i < c.Cluster.MasterCnt; i++ {
					masterFound := false
					curSlaves := sets.NewString()
					for j := 0; j <= c.Cluster.Replicas; j++ {
						if ips[i][j] == master.Ip {
							masterFound = true
							for _, slave := range master.Slaves {
								curSlaves.Insert(slave.Ip)
							}
							break
						}
						//slaves.Insert(ips[i][j])
					}

					if masterFound {
						for j := 0; j <= c.Cluster.Replicas; j++ {
							if ips[i][j] != master.Ip && !curSlaves.Has(ips[i][j]) {
								addNode(ips[i][j]+":6379", master.Ip+":6379", masterID)
							}
						}
						break
					}
				}
			}
		}
		//for i := 0; i < c.Cluster.MasterCnt; i++ {
		//	curMasterID := ""
		//	curMasterIP := ""
		//FindMaster:
		//	for j := 0; j <= c.Cluster.Replicas; j++ {
		//		//curIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
		//		//	c.BaseName, i, j, c.GoverningService, c.Namespace))
		//		//curIP := ips[i][j]
		//		for _, master := range nds {
		//			if master.Ip == ips[i][j] {
		//				curMasterIP = ips[i][j]
		//				curMasterID = master.Id
		//				break FindMaster
		//			}
		//		}
		//	}
		//
		//	for j := 0; j <= c.Cluster.Replicas; j++ {
		//		//curIp := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
		//		//	c.BaseName, i, j, c.GoverningService, c.Namespace))
		//		//curIP := ips[i][j]
		//		exists := false
		//	FindSlave:
		//		for _, slave := range nds[curMasterID].Slaves {
		//			if slave.Ip == ips[i][j] {
		//				exists = true
		//				break FindSlave
		//			}
		//		}
		//
		//		if !exists {
		//			addNode(ips[i][j]+":6379", curMasterIP+":6379", curMasterID)
		//		}
		//	}
		//}
	} else {
		c.createNewCluster()
	}

}

func (c Config) createNewCluster() {
	masterAddrs := make([]string, c.Cluster.MasterCnt)
	masterNodeIds := make([]string, c.Cluster.MasterCnt)
	ips := c.getIPs()

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local",
		//	c.BaseName, i, 0, c.GoverningService, c.Namespace))
		ip := ips[i][0]
		masterAddrs[i] = ip + ":6379"
		masterNodeIds[i] = getNodeId(getMyConf(getClusterNodes(ip)))
	}
	createCluster(masterAddrs...)

	for i := 0; i < c.Cluster.MasterCnt; i++ {
		for j := 1; j <= c.Cluster.Replicas; j++ {
			//ip := getIPByHostName(fmt.Sprintf("%s-shard%d-%d.%s.%s.svc.cluster.local", c.BaseName, i, j, c.GoverningService, c.Namespace))
			ip := ips[i][j]
			addNode(ip+":6379", masterAddrs[i], masterNodeIds[i])
		}
	}
}

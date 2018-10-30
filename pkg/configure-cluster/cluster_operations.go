package configure_cluster

import (
	"strconv"
	"strings"

	"github.com/appscode/go/log"
	"github.com/kubedb/redis/pkg/exec"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
)

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

func (c Config) clusterReset(pod *core.Pod, ip string) error {
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
		need int
		err  error
	)

	need = requstedSlotsCount

	for i := range nodes[src][0].SlotStart {
		if need <= 0 {
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

		need -= (end - start + 1)
	}

	return nil
}

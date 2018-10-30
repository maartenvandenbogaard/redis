package framework

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/appscode/go/sets"
	core_util "github.com/appscode/kutil/core/v1"
	"github.com/appscode/kutil/tools/portforward"
	rd "github.com/go-redis/redis"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/redis/test/e2e/util"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func (f *Framework) RedisClusterOptions() *rd.ClusterOptions {
	return &rd.ClusterOptions{
		DialTimeout:        10 * time.Second,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        500 * time.Millisecond,
		IdleCheckFrequency: 500 * time.Millisecond,
	}
}

type RedisNode struct {
	SlotStart []int
	SlotEnd   []int
	SlotsCnt  int

	ID       string
	IP       string
	Port     string
	Role     string
	Down     bool
	MasterID string

	Master *RedisNode
	Slaves []*RedisNode
}

func (f *Framework) GetPodsIPWithTunnel(redis *api.Redis) ([][]string, [][]*portforward.Tunnel, error) {
	return util.FowardedPodsIPWithTunnel(f.kubeClient, f.restConfig, redis)
}

func (f *Framework) Sync(addrs [][]string, redis *api.Redis) ([][]RedisNode, [][]*rd.Client) {
	var (
		nodes     = make([][]RedisNode, int(*redis.Spec.Cluster.Master))
		rdClients = make([][]*rd.Client, int(*redis.Spec.Cluster.Master))

		start, end int
		nodesConf  string
		slotRange  []string
		err        error
	)

	for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
		nodes[i] = make([]RedisNode, int(*redis.Spec.Cluster.Replicas)+1)
		rdClients[i] = make([]*rd.Client, int(*redis.Spec.Cluster.Replicas)+1)

		for j := 0; j <= int(*redis.Spec.Cluster.Replicas); j++ {
			rdClients[i][j] = rd.NewClient(&rd.Options{
				Addr: fmt.Sprintf(":%s", addrs[i][j]),
			})

			nodesConf, err = rdClients[i][j].ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())

			nodesConf = strings.TrimSpace(nodesConf)
			for _, info := range strings.Split(nodesConf, "\n") {
				info = strings.TrimSpace(info)

				if strings.Contains(info, "myself") {
					parts := strings.Split(info, " ")

					node := RedisNode{
						ID:   parts[0],
						IP:   strings.Split(parts[1], ":")[0],
						Port: addrs[i][j],
					}

					if strings.Contains(parts[2], "slave") {
						node.Role = "slave"
						node.MasterID = parts[3]
					} else {
						node.Role = "master"
						node.SlotsCnt = 0

						for k := 8; k < len(parts); k++ {
							if parts[k][0] == '[' && parts[k][len(parts[k])-1] == ']' {
								continue
							}

							slotRange = strings.Split(parts[k], "-")

							// slotRange contains only int. So errors are ignored
							start, _ = strconv.Atoi(slotRange[0])
							if len(slotRange) == 1 {
								end = start
							} else {
								end, _ = strconv.Atoi(slotRange[1])
							}

							node.SlotStart = append(node.SlotStart, start)
							node.SlotEnd = append(node.SlotEnd, end)
							node.SlotsCnt += (end - start) + 1
						}
					}
					nodes[i][j] = node
					break
				}
			}
		}
	}

	return nodes, rdClients
}

func (f *Framework) WaitUntilRedisClusterConfigured(redis *api.Redis, port string) error {
	return wait.PollImmediate(time.Second*5, time.Minute*5, func() (bool, error) {
		rdClient := rd.NewClient(&rd.Options{
			Addr: fmt.Sprintf(":%s", port),
		})

		slots, err := rdClient.ClusterSlots().Result()
		if err != nil {
			return false, nil
		}

		total := 0
		masterIds := sets.NewString()
		checkReplcas := true
		for _, slot := range slots {
			total += slot.End - slot.Start + 1
			masterIds.Insert(slot.Nodes[0].Id)
			checkReplcas = checkReplcas && (len(slot.Nodes)-1 == int(*redis.Spec.Cluster.Replicas))
		}

		if total != 16384 || masterIds.Len() != int(*redis.Spec.Cluster.Master) || !checkReplcas {
			return false, nil
		}

		return true, nil
	})
}

func (f *Framework) WaitUntilStatefulSetReady(redis *api.Redis) error {
	for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
		for j := 0; j <= int(*redis.Spec.Cluster.Replicas); j++ {
			podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)
			err := core_util.WaitUntilPodRunning(
				f.kubeClient,
				metav1.ObjectMeta{
					Name:      podName,
					Namespace: redis.Namespace,
				},
			)
			if err != nil {
				return errors.Wrapf(err, "failed to ready pod '%s/%s'", redis.Namespace, podName)
			}
		}
	}

	return nil
}

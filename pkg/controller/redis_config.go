package controller

import (
	"fmt"

	"github.com/appscode/go/types"
	core_util "github.com/appscode/kutil/core/v1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	kutildb "github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	"github.com/kubedb/apimachinery/pkg/eventer"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/reference"
)

const (
	RedisConfigKey          = "redis.conf"
	RedisConfigRelativePath = "redis.conf"

	RedisFixIPScriptKey          = "fix-ip.sh"
	RedisFixIPScriptRelativePath = "fix-ip.sh"
)

var redisConfig = `
cluster-enabled yes
cluster-config-file /data/nodes.conf
cluster-node-timeout 5000
cluster-migration-barrier 1
dir /data
appendonly yes
protected-mode no
`

// fixIPConfig script ensures the host ip in nodes.conf file is same as the IP of the pod (which is responsible
// for current redis server) in case of pod restart.
var fixIPConfig = `
CLUSTER_CONFIG="/data/nodes.conf"
if [ -f ${CLUSTER_CONFIG} ]; then
    if [ -z "${POD_IP}" ]; then
        echo "Unable to determine Pod IP address!"
        exit 1
    fi
    echo "Updating my IP to ${POD_IP} in ${CLUSTER_CONFIG}"
    sed -i.bak -e '/myself/ s/[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}/${POD_IP}/' ${CLUSTER_CONFIG}
fi

redis-server $REDIS_CONFIG`

func (c *Controller) ensureRedisConfig(redis *api.Redis) error {
	if redis.Spec.ConfigSource == nil {
		data := map[string]string{
			RedisConfigKey: redisConfig,
		}
		name := redis.ConfigMapName()
		cm, err := c.createConfigMap(name, data, redis)
		if err != nil {
			return err
		}

		redis.Spec.ConfigSource = &core.VolumeSource{}
		rd, _, err := kutildb.PatchRedis(c.ExtClient.KubedbV1alpha1(), redis, func(in *api.Redis) *api.Redis {
			in.Spec.ConfigSource = &core.VolumeSource{
				ConfigMap: &core.ConfigMapVolumeSource{
					LocalObjectReference: core.LocalObjectReference{
						Name: cm.Name,
					},
					DefaultMode: types.Int32P(511),
				},
			}
			return in
		})
		if err != nil {
			if ref, rerr := reference.GetReference(clientsetscheme.Scheme, redis); rerr == nil {
				c.recorder.Eventf(
					ref, core.EventTypeWarning,
					eventer.EventReasonFailedToUpdate,
					err.Error(),
				)
			}
			return err
		}
		redis.Spec.ConfigSource = rd.Spec.ConfigSource
	}

	//data := map[string]string{
	//	RedisFixIPScriptKey: fixIPConfig,
	//}
	//name := redis.Name + "-fixip"
	//_, err := c.createConfigMap(name, data, redis)
	//if err != nil {
	//	return err
	//}

	return nil
}

func (c *Controller) findConfigMap(configmapName string, redis *api.Redis) (*core.ConfigMap, error) {
	cm, err := c.Client.CoreV1().ConfigMaps(redis.Namespace).Get(configmapName, metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	if cm != nil && (cm.Labels[api.LabelDatabaseKind] != api.ResourceKindRedis ||
		cm.Labels[api.LabelDatabaseName] != redis.Name) {
		return nil, fmt.Errorf(`intended configmap "%v" already exists`, configmapName)
	}

	return cm, nil
}

func (c *Controller) createConfigMap(name string, data map[string]string, redis *api.Redis) (*core.ConfigMap, error) {
	cm, err := c.findConfigMap(name, redis)
	if err != nil {
		return nil, err
	}

	ref, rerr := reference.GetReference(clientsetscheme.Scheme, redis)
	if rerr != nil {
		return nil, rerr
	}
	if cm == nil {
		cmObj := &core.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: redis.Namespace,
				Labels: map[string]string{
					api.LabelDatabaseKind: api.ResourceKindRedis,
					api.LabelDatabaseName: redis.Name,
				},
			},
			Data: data,
		}
		core_util.EnsureOwnerReference(&cmObj.ObjectMeta, ref)
		cm, err = c.Client.CoreV1().ConfigMaps(redis.Namespace).Create(cmObj)
		if err != nil {
			return nil, err
		}
	}

	return cm, nil
}

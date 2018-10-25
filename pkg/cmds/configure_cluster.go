package cmds

import (
	"github.com/appscode/go/log"
	rd_cluster "github.com/kubedb/redis/pkg/configure-cluster"
	"github.com/spf13/cobra"
)

func NewCmdConfigureCluster() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "configure-cluster",
		Short:             "Run configure-cluster cmd to configure redis cluster",
		DisableAutoGenTag: true,
		Run: func(cmd *cobra.Command, args []string) {
			log.Infoln("Configuring redis-servers as a cluster...")

			rd_cluster.ConfigureRedisCluster()
		},
	}

	return cmd
}

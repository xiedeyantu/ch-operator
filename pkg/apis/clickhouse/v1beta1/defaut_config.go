package v1beta1

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	DefaultZkContainerRepository = "zookeeper"
	DefaultZkContainerVersion    = "3.6.1"
	DefaultZkContainerPolicy     = "Always"

	DefaultChContainerRepository = "xiedeyantu/clickhouse-server"
	DefaultChContainerVersion    = "20.3.18.10"
	DefaultChContainerPolicy     = "Always"

	DefaultTerminationGracePeriod   = 30
	DefaultZookeeperCacheVolumeSize = "20Gi"
)

const (
	VolumeReclaimPolicyRetain VolumeReclaimPolicy = "Retain"
	VolumeReclaimPolicyDelete VolumeReclaimPolicy = "Delete"
)

func (c *ClickHouseCluster) GetChName() string {
	return fmt.Sprintf("%s-%s", c.Name, c.Spec.ClickHouse.Name)
}

func (c *ClickHouseCluster) GetChStatefulSetName(shard, replica int32) string {
	return fmt.Sprintf("%s-%s-%d-%d", c.Name, c.Spec.ClickHouse.Name, shard, replica)
}

func (c *ClickHouseCluster) GetZkName() string {
	return fmt.Sprintf("%s-%s", c.Name, c.Spec.Zookeeper.Name)
}

// ConfigMapName returns the name of the cluster config-map
func (z *ClickHouseCluster) ConfigMapName() string {
	return fmt.Sprintf("%s-configmap", z.Spec.Zookeeper.Name)
}

func FillChContainerPortsWithDefaults(cp []v1.ContainerPort) (containerPorts []v1.ContainerPort) {
	if len(cp) == 0 {
		containerPorts = []v1.ContainerPort{
			{
				Name:          "tcp",
				ContainerPort: 9000,
			},
			{
				Name:          "http",
				ContainerPort: 8123,
			},
			{
				Name:          "inner-server",
				ContainerPort: 9009,
			},
		}
		return containerPorts
	} else {
		for _, p := range cp {
			containerPorts = append(containerPorts, v1.ContainerPort{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
			})
		}
		return containerPorts
	}
}

func FillChServicePortsWithDefaults(sp []v1.ContainerPort) (servicePorts []v1.ServicePort) {
	if len(sp) == 0 {
		servicePorts = []v1.ServicePort{
			{
				Name: "tcp",
				Port: 9000,
			},
			{
				Name: "http",
				Port: 8123,
			},
			{
				Name: "inner-server",
				Port: 9009,
			},
		}
		return servicePorts
	} else {
		for _, s := range sp {
			servicePorts = append(servicePorts, v1.ServicePort{
				Name: s.Name,
				Port: s.ContainerPort,
			})
		}
		return servicePorts
	}
}

func FillZkContainerPortsWithDefaults(cp []v1.ContainerPort) (containerPorts []v1.ContainerPort) {
	if len(cp) == 0 {
		containerPorts = []v1.ContainerPort{
			{
				Name:          "client",
				ContainerPort: 2181,
			},
			{
				Name:          "quorum",
				ContainerPort: 2888,
			},
			{
				Name:          "leader-election",
				ContainerPort: 3888,
			},
			{
				Name:          "metrics",
				ContainerPort: 7000,
			},
		}
		return containerPorts
	} else {
		for _, p := range cp {
			containerPorts = append(containerPorts, v1.ContainerPort{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
			})
		}
		return containerPorts
	}
}

func FillZkServicePortsWithDefaults(sp []v1.ContainerPort) (servicePorts []v1.ServicePort) {
	if len(sp) == 0 {
		servicePorts = []v1.ServicePort{
			{
				Name: "client",
				Port: 2181,
			},
			{
				Name: "quorum",
				Port: 2888,
			},
			{
				Name: "leader-election",
				Port: 3888,
			},
			{
				Name: "metrics",
				Port: 7000,
			},
		}
		return servicePorts
	} else {
		for _, s := range sp {
			servicePorts = append(servicePorts, v1.ServicePort{
				Name: s.Name,
				Port: s.ContainerPort,
			})
		}
		return servicePorts
	}
}

func (p *Persistence) WithDefaults() {
	if !p.VolumeReclaimPolicy.isValid() {
		p.VolumeReclaimPolicy = VolumeReclaimPolicyRetain
	}
	if p.VolumeReclaimPolicy != VolumeReclaimPolicyDelete &&
		p.VolumeReclaimPolicy != VolumeReclaimPolicyRetain {
		p.VolumeReclaimPolicy = VolumeReclaimPolicyRetain
	}
	p.PersistentVolumeClaimSpec.AccessModes = []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}

	storage, _ := p.PersistentVolumeClaimSpec.Resources.Requests["storage"]
	if storage.IsZero() {
		p.PersistentVolumeClaimSpec.Resources.Requests = v1.ResourceList{
			v1.ResourceStorage: resource.MustParse(DefaultZookeeperCacheVolumeSize),
		}
	}
}

func (c *ContainerImage) WithChDefaults() {
	if c.Repository == "" {
		c.Repository = DefaultChContainerRepository
	}
	if c.Tag == "" {
		c.Tag = DefaultChContainerVersion
	}
	if c.PullPolicy == "" {
		c.PullPolicy = DefaultChContainerPolicy
	}
}

func (c *ContainerImage) WithZkDefaults() {
	if c.Repository == "" {
		c.Repository = DefaultZkContainerRepository
	}
	if c.Tag == "" {
		c.Tag = DefaultZkContainerVersion
	}
	if c.PullPolicy == "" {
		c.PullPolicy = DefaultZkContainerPolicy
	}
}

func (c *ZookeeperConfig) WithDefaults() {
	if c.InitLimit == 0 {
		c.InitLimit = 30000
	}
	if c.TickTime == 0 {
		c.TickTime = 2000
	}
	if c.SyncLimit == 0 {
		c.SyncLimit = 10
	}
	if c.GlobalOutstandingLimit == 0 {
		c.GlobalOutstandingLimit = 1000
	}
	if c.PreAllocSize == 0 {
		c.PreAllocSize = 65536
	}
	if c.SnapCount == 0 {
		c.SnapCount = 10000
	}
	if c.CommitLogCount == 0 {
		c.CommitLogCount = 500
	}
	if c.SnapSizeLimitInKb == 0 {
		c.SnapSizeLimitInKb = 4194304
	}
	if c.MaxClientCnxns == 0 {
		c.MaxClientCnxns = 60
	}
	if c.MinSessionTimeout == 0 {
		c.MinSessionTimeout = 2 * c.TickTime
	}
	if c.MaxSessionTimeout == 0 {
		c.MaxSessionTimeout = 20 * c.TickTime
	}
	if c.AutoPurgeSnapRetainCount == 0 {
		c.AutoPurgeSnapRetainCount = 3
	}
	if c.AutoPurgePurgeInterval == 0 {
		c.AutoPurgePurgeInterval = 1
	}
	if !c.QuorumListenOnAllIPs {
		c.QuorumListenOnAllIPs = true
	}
}

func (v VolumeReclaimPolicy) isValid() bool {
	if v != VolumeReclaimPolicyDelete && v != VolumeReclaimPolicyRetain {
		return false
	}
	return true
}

func (c *ContainerImage) ToString() string {
	return fmt.Sprintf("%s:%s", c.Repository, c.Tag)
}

func GenerateZkDomain(c *ClickHouseCluster, index int32) (ZkDomain string) {
	ZkDomain = fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local",
		c.GetZkName(),
		index,
		fmt.Sprintf("%s-zk", c.GetZkName()),
		c.GetNamespace())
	return ZkDomain
}

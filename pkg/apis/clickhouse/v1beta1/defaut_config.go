package v1beta1

const (
	DefaultZkContainerRepository = "zookeeper"
	DefaultZkContainerVersion = "3.6.1"
	DefaultZkContainerPolicy = "Always"

	DefaultChContainerRepository = "xiedeyantu/clickhouse-server"
	DefaultChContainerVersion = "20.3.18.10"
	DefaultChContainerPolicy = "Always"

	DefaultTerminationGracePeriod = 30
	DefaultZookeeperCacheVolumeSize = "20Gi"
)

const (
	VolumeReclaimPolicyRetain VolumeReclaimPolicy = "Retain"
	VolumeReclaimPolicyDelete VolumeReclaimPolicy = "Delete"
)


func (p *Persistence) withDefaults() (changed bool) {
	if !p.VolumeReclaimPolicy.IsValid() {
		changed = true
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
		changed = true
	}
	return changed
}
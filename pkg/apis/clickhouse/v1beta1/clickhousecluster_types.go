package v1beta1

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const (
	// DefaultZkContainerRepository is the default docker repo for the zookeeper
	// container
	DefaultZkContainerRepository = "zookeeper"

	// DefaultZkContainerVersion is the default tag used for for the zookeeper
	// container
	DefaultZkContainerVersion = "3.6.1"

	// DefaultZkContainerPolicy is the default container pull policy used
	DefaultZkContainerPolicy = "Always"

	// DefaultTerminationGracePeriod is the default time given before the
	// container is stopped. This gives clients time to disconnect from a
	// specific node gracefully.
	DefaultTerminationGracePeriod = 30

	// DefaultZookeeperCacheVolumeSize is the default volume size for the
	// Zookeeper cache volume
	DefaultZookeeperCacheVolumeSize = "20Gi"
)

// ClickHouseClusterSpec defines the desired state of ClickHouseCluster
type ClickHouseClusterSpec struct {
	Zookeeper  Zookeeper  `json:"zookeeper,omitempty"`
	ClickHouse ClickHouse `json:"clickhouse,omitempty"`
}

type Zookeeper struct {
	Name string `json:"name"`
	// Image is the  container image. default is zookeeper:0.2.7
	Image ContainerImage `json:"image,omitempty"`

	// Labels specifies the labels to attach to pods the operator creates for
	// the zookeeper cluster.
	Labels map[string]string `json:"labels,omitempty"`

	// Replicas is the expected size of the zookeeper cluster.
	// The pravega-operator will eventually make the size of the running cluster
	// equal to the expected size.
	//
	// The valid range of size is from 1 to 7.
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`

	Ports []v1.ContainerPort `json:"ports,omitempty"`

	// Pod defines the policy to create pod for the zookeeper cluster.
	// Updating the Pod does not take effect on any existing pods.
	Pod PodPolicy `json:"pod,omitempty"`

	//StorageType is used to tell which type of storage we will be using
	//It can take either Ephemeral or persistence
	//Default StorageType is Persistence storage
	StorageType string `json:"storageType,omitempty"`

	// Persistence is the configuration for zookeeper persistent layer.
	// PersistentVolumeClaimSpec and VolumeReclaimPolicy can be specified in here.
	Persistence *Persistence `json:"persistence,omitempty"`

	// Ephemeral is the configuration which helps create ephemeral storage
	// At anypoint only one of Persistence or Ephemeral should be present in the manifest
	Ephemeral *Ephemeral `json:"ephemeral,omitempty"`

	// Conf is the zookeeper configuration, which will be used to generate the
	// static zookeeper configuration. If no configuration is provided required
	// default values will be provided, and optional values will be excluded.
	Conf ZookeeperConfig `json:"config,omitempty"`

	// External host name appended for dns annotation
	DomainName string `json:"domainName,omitempty"`

	// Domain of the kubernetes cluster, defaults to cluster.local
	KubernetesClusterDomain string `json:"kubernetesClusterDomain,omitempty"`

	// Containers defines to support multi containers
	Containers []v1.Container `json:"containers,omitempty"`

	// Volumes defines to support customized volumes
	Volumes []v1.Volume `json:"volumes,omitempty"`
}

type ClickHouse struct {
	Name string `json:"name"`

	Image ContainerImage `json:"image,omitempty"`

	Labels map[string]string `json:"labels,omitempty"`

	Shards int32 `json:"shards,omitempty"`

	Replicas int32 `json:"replicas,omitempty"`

	Ports []v1.ContainerPort `json:"ports,omitempty"`

	Pod PodPolicy `json:"pod,omitempty"`

	StorageType string `json:"storageType,omitempty"`

	Persistence *Persistence `json:"persistence,omitempty"`

	Ephemeral *Ephemeral `json:"ephemeral,omitempty"`

	//Conf ClickHouseConfig `json:"config,omitempty"`

	// External host name appended for dns annotation
	DomainName string `json:"domainName,omitempty"`

	// Domain of the kubernetes cluster, defaults to cluster.local
	//KubernetesClusterDomain string `json:"kubernetesClusterDomain,omitempty"`

	// Containers defines to support multi containers
	Containers []v1.Container `json:"containers,omitempty"`

	// Volumes defines to support customized volumes
	Volumes []v1.Volume `json:"volumes,omitempty"`
}

type ContainerImage struct {
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	// +kubebuilder:validation:Enum="Always";"Never";"IfNotPresent"
	PullPolicy v1.PullPolicy `json:"pullPolicy,omitempty"`
}

type PodPolicy struct {
	// Labels specifies the labels to attach to pods the operator creates for
	// the zookeeper cluster.
	Labels map[string]string `json:"labels,omitempty"`

	// NodeSelector specifies a map of key-value pairs. For the pod to be
	// eligible to run on a node, the node must have each of the indicated
	// key-value pairs as labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// The scheduling constraints on pods.
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// Resources is the resource requirements for the container.
	// This field cannot be updated once the cluster is created.
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations specifies the pod's tolerations.
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// List of environment variables to set in the container.
	// This field cannot be updated.
	Env []v1.EnvVar `json:"env,omitempty"`

	// Annotations specifies the annotations to attach to pods the operator
	// creates.
	Annotations map[string]string `json:"annotations,omitempty"`

	// SecurityContext specifies the security context for the entire pod
	// More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context
	SecurityContext *v1.PodSecurityContext `json:"securityContext,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// TerminationGracePeriodSeconds is the amount of time that kubernetes will
	// give for a pod instance to shutdown normally.
	// The default value is 30.
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// Service Account to be used in pods
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// ImagePullSecrets is a list of references to secrets in the same namespace to use for pulling any images
	ImagePullSecrets []v1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

type Persistence struct {
	// VolumeReclaimPolicy is a zookeeper operator configuration. If it's set to Delete,
	// the corresponding PVCs will be deleted by the operator when zookeeper cluster is deleted.
	// The default value is Retain.
	// +kubebuilder:validation:Enum="Delete";"Retain"
	VolumeReclaimPolicy VolumeReclaimPolicy `json:"reclaimPolicy,omitempty"`
	// PersistentVolumeClaimSpec is the spec to describe PVC for the container
	// This field is optional. If no PVC is specified default persistentvolume
	// will get created.
	PersistentVolumeClaimSpec v1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
	// Annotations specifies the annotations to attach to pvc the operator
	// creates.
	Annotations map[string]string `json:"annotations,omitempty"`
}

type VolumeReclaimPolicy string

const (
	VolumeReclaimPolicyRetain VolumeReclaimPolicy = "Retain"
	VolumeReclaimPolicyDelete VolumeReclaimPolicy = "Delete"
)

type Ephemeral struct {
	//EmptyDirVolumeSource is optional and this will create the emptydir volume
	//It has two parameters Medium and SizeLimit which are optional as well
	//Medium specifies What type of storage medium should back this directory.
	//SizeLimit specifies Total amount of local storage required for this EmptyDir volume.
	EmptyDirVolumeSource v1.EmptyDirVolumeSource `json:"emptydirvolumesource,omitempty"`
}

type ZookeeperConfig struct {
	// InitLimit is the amount of time, in ticks, to allow followers to connect
	// and sync to a leader.
	//
	// Default value is 10.
	InitLimit int `json:"initLimit,omitempty"`

	// TickTime is the length of a single tick, which is the basic time unit used
	// by Zookeeper, as measured in milliseconds
	//
	// The default value is 2000.
	TickTime int `json:"tickTime,omitempty"`

	// SyncLimit is the amount of time, in ticks, to allow followers to sync with
	// Zookeeper.
	//
	// The default value is 2.
	SyncLimit int `json:"syncLimit,omitempty"`

	// Clients can submit requests faster than ZooKeeper can process them, especially
	// if there are a lot of clients. Zookeeper will throttle Clients so that requests
	// won't exceed global outstanding limit.
	//
	// The default value is 1000
	GlobalOutstandingLimit int `json:"globalOutstandingLimit,omitempty"`

	// To avoid seeks ZooKeeper allocates space in the transaction log file in
	// blocks of preAllocSize kilobytes
	//
	// The default value is 64M
	PreAllocSize int `json:"preAllocSize,omitempty"`

	// ZooKeeper records its transactions using snapshots and a transaction log
	// The number of transactions recorded in the transaction log before a snapshot
	// can be taken is determined by snapCount
	//
	// The default value is 100,000
	SnapCount int `json:"snapCount,omitempty"`

	// Zookeeper maintains an in-memory list of last committed requests for fast
	// synchronization with followers
	//
	// The default value is 500
	CommitLogCount int `json:"commitLogCount,omitempty"`

	// Snapshot size limit in Kb
	//
	// The defult value is 4GB
	SnapSizeLimitInKb int `json:"snapSizeLimitInKb,omitempty"`

	// Limits the total number of concurrent connections that can be made to a
	//zookeeper server
	//
	// The defult value is 0, indicating no limit
	MaxCnxns int `json:"maxCnxns,omitempty"`

	// Limits the number of concurrent connections that a single client, identified
	// by IP address, may make to a single member of the ZooKeeper ensemble.
	//
	// The default value is 60
	MaxClientCnxns int `json:"maxClientCnxns,omitempty"`

	// The minimum session timeout in milliseconds that the server will allow the
	// client to negotiate
	//
	// The default value is 4000
	MinSessionTimeout int `json:"minSessionTimeout,omitempty"`

	// The maximum session timeout in milliseconds that the server will allow the
	// client to negotiate.
	//
	// The default value is 40000
	MaxSessionTimeout int `json:"maxSessionTimeout,omitempty"`

	// Retain the snapshots according to retain count
	//
	// The default value is 3
	AutoPurgeSnapRetainCount int `json:"autoPurgeSnapRetainCount,omitempty"`

	// The time interval in hours for which the purge task has to be triggered
	//
	// Disabled by default
	AutoPurgePurgeInterval int `json:"autoPurgePurgeInterval,omitempty"`

	// QuorumListenOnAllIPs when set to true the ZooKeeper server will listen for
	// connections from its peers on all available IP addresses, and not only the
	// address configured in the server list of the configuration file. It affects
	// the connections handling the ZAB protocol and the Fast Leader Election protocol.
	//
	// The default value is false.
	QuorumListenOnAllIPs bool `json:"quorumListenOnAllIPs,omitempty"`
}

// ClickHouseClusterStatus defines the observed state of ClickHouseCluster
type ClickHouseClusterStatus struct {
	// Replicas is the number of number of desired replicas in the cluster
	ZkReplicas int32 `json:"zkReplicas,omitempty"`

	// ReadyReplicas is the number of number of ready replicas in the cluster
	ZkReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Conditions list all the applied conditions
	Conditions []ClusterCondition `json:"conditions,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseCluster is the Schema for the clickhouseclusters API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=clickhouseclusters,scope=Namespaced
type ClickHouseCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClickHouseClusterSpec   `json:"spec,omitempty"`
	Status ClickHouseClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseClusterList contains a list of ClickHouseCluster
type ClickHouseClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClickHouseCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClickHouseCluster{}, &ClickHouseClusterList{})
}

func (p *PodPolicy) withDefaults(z *ClickHouseCluster) (changed bool) {
	if p.Labels == nil {
		p.Labels = map[string]string{}
		changed = true
	}
	if p.TerminationGracePeriodSeconds == 0 {
		p.TerminationGracePeriodSeconds = DefaultTerminationGracePeriod
		changed = true
	}
	if p.ServiceAccountName == "" {
		p.ServiceAccountName = "default"
		changed = true
	}
	if z.Spec.Zookeeper.Pod.Labels == nil {
		p.Labels = map[string]string{}
		changed = true
	}
	if _, ok := p.Labels["app"]; !ok {
		p.Labels["app"] = z.Spec.Zookeeper.Name
		changed = true
	}
	if _, ok := p.Labels["release"]; !ok {
		p.Labels["release"] = z.Spec.Zookeeper.Name
		changed = true
	}
	if p.Affinity == nil {
		p.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
					{
						Weight: 20,
						PodAffinityTerm: v1.PodAffinityTerm{
							TopologyKey: "kubernetes.io/hostname",
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "app",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{z.Spec.Zookeeper.Name},
									},
								},
							},
						},
					},
				},
			},
		}
		changed = true
	}
	return changed
}

func (s *Zookeeper) withDefaults(z *ClickHouseCluster) (changed bool) {
	changed = s.Image.withDefaults()
	if s.Conf.withDefaults() {
		changed = true
	}
	if s.Replicas == 0 {
		s.Replicas = 3
		changed = true
	}
	if s.Ports == nil {
		s.Ports = []v1.ContainerPort{
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
		changed = true
	} else {
		var (
			foundClient, foundQuorum, foundLeader, foundMetrics bool
		)
		for i := 0; i < len(s.Ports); i++ {
			if s.Ports[i].Name == "client" {
				foundClient = true
			} else if s.Ports[i].Name == "quorum" {
				foundQuorum = true
			} else if s.Ports[i].Name == "leader-election" {
				foundLeader = true
			} else if s.Ports[i].Name == "metrics" {
				foundMetrics = true
			}
		}
		if !foundClient {
			ports := v1.ContainerPort{Name: "client", ContainerPort: 2181}
			s.Ports = append(s.Ports, ports)
			changed = true
		}
		if !foundQuorum {
			ports := v1.ContainerPort{Name: "quorum", ContainerPort: 2888}
			s.Ports = append(s.Ports, ports)
			changed = true
		}
		if !foundLeader {
			ports := v1.ContainerPort{Name: "leader-election", ContainerPort: 3888}
			s.Ports = append(s.Ports, ports)
			changed = true
		}
		if !foundMetrics {
			ports := v1.ContainerPort{Name: "metrics", ContainerPort: 7000}
			s.Ports = append(s.Ports, ports)
			changed = true
		}
	}

	if z.Spec.Zookeeper.Labels == nil {
		z.Spec.Zookeeper.Labels = map[string]string{}
		changed = true
	}
	if _, ok := z.Spec.Zookeeper.Labels["app"]; !ok {
		z.Spec.Zookeeper.Labels["app"] = z.Spec.Zookeeper.Name
		changed = true
	}
	if _, ok := z.Spec.Zookeeper.Labels["release"]; !ok {
		z.Spec.Zookeeper.Labels["release"] = z.Spec.Zookeeper.Name
		changed = true
	}
	if s.Pod.withDefaults(z) {
		changed = true
	}
	if strings.EqualFold(s.StorageType, "ephemeral") {
		if s.Ephemeral == nil {
			s.Ephemeral = &Ephemeral{}
			s.Ephemeral.EmptyDirVolumeSource = v1.EmptyDirVolumeSource{}
			changed = true
		}
	} else {
		if s.Persistence == nil {
			s.StorageType = "persistence"
			s.Persistence = &Persistence{}
			changed = true
		}
		if s.Persistence.withDefaults() {
			s.StorageType = "persistence"
			changed = true
		}
	}
	return changed
}

// WithDefaults set default values when not defined in the spec.
func (z *ClickHouseCluster) WithDefaults() bool {
	return z.Spec.Zookeeper.withDefaults(z)
}

// ConfigMapName returns the name of the cluster config-map
func (z *ClickHouseCluster) ConfigMapName() string {
	return fmt.Sprintf("%s-configmap", z.Spec.Zookeeper.Name)
}

// GetKubernetesClusterDomain returns the cluster domain of kubernetes
func (z *ClickHouseCluster) GetKubernetesClusterDomain() string {
	if z.Spec.Zookeeper.KubernetesClusterDomain == "" {
		return "cluster.local"
	}
	return z.Spec.Zookeeper.KubernetesClusterDomain
}

func (z *ClickHouseCluster) ClickHousePorts() map[string]int32 {
	ports := map[string]int32{}
	for _, p := range z.Spec.ClickHouse.Ports {
		ports[p.Name] = p.ContainerPort
	}
	return ports
}

// ZookeeperPorts returns a struct of ports
func (z *ClickHouseCluster) ZookeeperPorts() Ports {
	ports := Ports{}
	for _, p := range z.Spec.Zookeeper.Ports {
		if p.Name == "client" {
			ports.Client = p.ContainerPort
		} else if p.Name == "quorum" {
			ports.Quorum = p.ContainerPort
		} else if p.Name == "leader-election" {
			ports.Leader = p.ContainerPort
		} else if p.Name == "metrics" {
			ports.Metrics = p.ContainerPort
		}
	}
	return ports
}

// GetClientServiceName returns the name of the client service for the cluster
//func (z *ClickHouseCluster) GetClientServiceName() string {
//	return fmt.Sprintf("%s-client", z.GetName())
//}

// Ports groups the ports for a zookeeper cluster node for easy access
type Ports struct {
	Client  int32
	Quorum  int32
	Leader  int32
	Metrics int32
}

func (c *ContainerImage) withDefaults() (changed bool) {
	if c.Repository == "" {
		changed = true
		c.Repository = DefaultZkContainerRepository
	}
	if c.Tag == "" {
		changed = true
		c.Tag = DefaultZkContainerVersion
	}
	if c.PullPolicy == "" {
		changed = true
		c.PullPolicy = DefaultZkContainerPolicy
	}
	return changed
}

// ToString formats a container image struct as a docker compatible repository
// string.
func (c *ContainerImage) ToString() string {
	return fmt.Sprintf("%s:%s", c.Repository, c.Tag)
}

func (c *ZookeeperConfig) withDefaults() (changed bool) {
	if c.InitLimit == 0 {
		changed = true
		c.InitLimit = 30000
	}
	if c.TickTime == 0 {
		changed = true
		c.TickTime = 2000
	}
	if c.SyncLimit == 0 {
		changed = true
		c.SyncLimit = 2
	}
	if c.GlobalOutstandingLimit == 0 {
		changed = true
		c.GlobalOutstandingLimit = 1000
	}
	if c.PreAllocSize == 0 {
		changed = true
		c.PreAllocSize = 65536
	}
	if c.SnapCount == 0 {
		changed = true
		c.SnapCount = 10000
	}
	if c.CommitLogCount == 0 {
		changed = true
		c.CommitLogCount = 500
	}
	if c.SnapSizeLimitInKb == 0 {
		changed = true
		c.SnapSizeLimitInKb = 4194304
	}
	if c.MaxClientCnxns == 0 {
		changed = true
		c.MaxClientCnxns = 60
	}
	if c.MinSessionTimeout == 0 {
		changed = true
		c.MinSessionTimeout = 2 * c.TickTime
	}
	if c.MaxSessionTimeout == 0 {
		changed = true
		c.MaxSessionTimeout = 20 * c.TickTime
	}
	if c.AutoPurgeSnapRetainCount == 0 {
		changed = true
		c.AutoPurgeSnapRetainCount = 3
	}
	if c.AutoPurgePurgeInterval == 0 {
		changed = true
		c.AutoPurgePurgeInterval = 1
	}

	return changed
}

func (p *Persistence) withDefaults() (changed bool) {
	if !p.VolumeReclaimPolicy.isValid() {
		changed = true
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

func (v VolumeReclaimPolicy) isValid() bool {
	if v != VolumeReclaimPolicyDelete && v != VolumeReclaimPolicyRetain {
		return false
	}
	return true
}

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClickHouseClusterSpec defines the desired state of ClickHouseCluster
type ClickHouseClusterSpec struct {
	Zookeeper  Zookeeper  `json:"zookeeper,omitempty"`
	ClickHouse ClickHouse `json:"clickhouse,omitempty"`
}

type Zookeeper struct {
	Name        string                  `json:"name"`
	Image       ContainerImage          `json:"image,omitempty"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Replicas    int32                   `json:"replicas,omitempty"`
	Ports       []v1.ContainerPort      `json:"ports,omitempty"`
	StorageType string                  `json:"storageType,omitempty"`
	Resources   v1.ResourceRequirements `json:"resources,omitempty"`
	Persistence *Persistence            `json:"persistence,omitempty"`
	Conf        ZookeeperConfig         `json:"config,omitempty"`
	Volumes     []v1.Volume             `json:"volumes,omitempty"`
}

type ClickHouse struct {
	Name        string                  `json:"name"`
	Image       ContainerImage          `json:"image,omitempty"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Shards      int32                   `json:"shards,omitempty"`
	Replicas    int32                   `json:"replicas,omitempty"`
	Ports       []v1.ContainerPort      `json:"ports,omitempty"`
	StorageType string                  `json:"storageType,omitempty"`
	Resources   v1.ResourceRequirements `json:"resources,omitempty"`
	Persistence *Persistence            `json:"persistence,omitempty"`
	Volumes     []v1.Volume             `json:"volumes,omitempty"`
}

type ContainerImage struct {
	Repository string        `json:"repository,omitempty"`
	Tag        string        `json:"tag,omitempty"`
	PullPolicy v1.PullPolicy `json:"pullPolicy,omitempty"`
}

type Persistence struct {
	VolumeReclaimPolicy       VolumeReclaimPolicy          `json:"reclaimPolicy,omitempty"`
	PersistentVolumeClaimSpec v1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
	Annotations               map[string]string            `json:"annotations,omitempty"`
}

type VolumeReclaimPolicy string

type Ephemeral struct {
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
	ZkReplicas       int32              `json:"zkReplicas,omitempty"`
	ZkReadyReplicas  int32              `json:"zkReadyReplicas,omitempty"`
	ChReplicas       int32              `json:"chReplicas,omitempty"`
	ChReadyReplicas  int32              `json:"chReadyReplicas,omitempty"`
	ChShardNum       int32              `json:"chShardNum,omitempty"`
	ChReplicaNum     int32              `json:"chReplicaNum,omitempty"`
	Conditions       []ClusterCondition `json:"conditions,omitempty"`
	ZookeeperStatus  ZookeeperStatus    `json:"zookeeperStatus,omitempty"`
	ClickHouseStatus ClickHouseStatus   `json:"clickhouseStatus,omitempty"`
}

type ZookeeperStatus struct {
	Resources v1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`
	Volumes   []v1.Volume             `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,9,rep,name=volumeMounts"`
}

type ClickHouseStatus struct {
	Resources v1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`
	Volumes   []v1.Volume             `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,9,rep,name=volumeMounts"`
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

// ClickHouseClusterList contains a list of ClickHouseCluster
type ClickHouseClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClickHouseCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClickHouseCluster{}, &ClickHouseClusterList{})
}

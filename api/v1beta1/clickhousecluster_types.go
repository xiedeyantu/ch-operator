/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ClickHouseClusterSpec defines the desired state of ClickHouseCluster
type ClickHouseClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

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
	InitLimit                int  `json:"initLimit,omitempty"`
	TickTime                 int  `json:"tickTime,omitempty"`
	SyncLimit                int  `json:"syncLimit,omitempty"`
	GlobalOutstandingLimit   int  `json:"globalOutstandingLimit,omitempty"`
	PreAllocSize             int  `json:"preAllocSize,omitempty"`
	SnapCount                int  `json:"snapCount,omitempty"`
	CommitLogCount           int  `json:"commitLogCount,omitempty"`
	SnapSizeLimitInKb        int  `json:"snapSizeLimitInKb,omitempty"`
	MaxCnxns                 int  `json:"maxCnxns,omitempty"`
	MaxClientCnxns           int  `json:"maxClientCnxns,omitempty"`
	MinSessionTimeout        int  `json:"minSessionTimeout,omitempty"`
	MaxSessionTimeout        int  `json:"maxSessionTimeout,omitempty"`
	AutoPurgeSnapRetainCount int  `json:"autoPurgeSnapRetainCount,omitempty"`
	AutoPurgePurgeInterval   int  `json:"autoPurgePurgeInterval,omitempty"`
	QuorumListenOnAllIPs     bool `json:"quorumListenOnAllIPs,omitempty"`
}

type ZookeeperStatus struct {
	Resources   v1.ResourceRequirements `json:"resources,omitempty"`
	Persistence *Persistence            `json:"persistence,omitempty"`
}

type ClickHouseStatus struct {
	Shards      int32                   `json:"chShardNum,omitempty"`
	Replicas    int32                   `json:"chReplicaNum,omitempty"`
	Resources   v1.ResourceRequirements `json:"resources,omitempty"`
	Persistence *Persistence            `json:"persistence,omitempty"`
}

// ClickHouseClusterStatus defines the observed state of ClickHouseCluster
type ClickHouseClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	ZkReplicas       int32              `json:"zkReplicas,omitempty"`
	ZkReadyReplicas  int32              `json:"zkReadyReplicas,omitempty"`
	ChNodes          int32              `json:"chReplicas,omitempty"`
	ChReadyNodes     int32              `json:"chReadyReplicas,omitempty"`
	Conditions       []ClusterCondition `json:"conditions,omitempty"`
	ZookeeperStatus  ZookeeperStatus    `json:"zookeeperStatus,omitempty"`
	ClickHouseStatus ClickHouseStatus   `json:"clickhouseStatus,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// ClickHouseCluster is the Schema for the clickhouseclusters API
type ClickHouseCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClickHouseClusterSpec   `json:"spec,omitempty"`
	Status ClickHouseClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ClickHouseClusterList contains a list of ClickHouseCluster
type ClickHouseClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClickHouseCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClickHouseCluster{}, &ClickHouseClusterList{})
}

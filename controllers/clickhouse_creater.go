package controllers

import (
	"github.com/xiedeyantu/ch-operator/api/v1beta1"
	"github.com/xiedeyantu/ch-operator/common"
	"github.com/xiedeyantu/ch-operator/controllers/config"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strings"
)

func MakeChConfigMap(c *v1beta1.ClickHouseCluster) *v1.ConfigMap {
	var zkDomain []string
	for i := int32(0); i < c.Spec.Zookeeper.Replicas; i++ {
		zkDomain = append(zkDomain, v1beta1.GenerateZkDomain(c, i))
	}
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.GetChName(),
			Namespace: c.Namespace,
			Labels:    c.Spec.Zookeeper.Labels,
		},
		Data: map[string]string{
			"zookeeper.xml":   config.GenerateChZkConfig(zkDomain),
			"host-listen.xml": config.GenerateChListenConfig(),
			"remote-servers.xml": config.GenerateChRemoteConfig(c.Spec.ClickHouse.Shards,
				c.Spec.ClickHouse.Replicas,
				c.GetChName()),
		},
	}
}

func MakeChPrivateConfigMap(c *v1beta1.ClickHouseCluster, shard, replica int32) *v1.ConfigMap {
	var zkDomain []string
	for i := int32(0); i < c.Spec.Zookeeper.Replicas; i++ {
		zkDomain = append(zkDomain, v1beta1.GenerateZkDomain(c, i))
	}
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.GetChStatefulSetName(shard, replica),
			Namespace: c.Namespace,
			Labels:    c.Spec.ClickHouse.Labels,
		},
		Data: map[string]string{
			"macros.xml": config.GenerateChMacrosConfig(shard, c.GetChStatefulSetName(shard, replica)),
		},
	}
}

func MakeChHeadlessService(c *v1beta1.ClickHouseCluster, shard, replica int32) *v1.Service {
	var annotationMap map[string]string
	svcName := c.GetChStatefulSetName(shard, replica)
	svcPorts := v1beta1.FillChServicePortsWithDefaults(c.Spec.ClickHouse.Ports)
	service := v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: c.Namespace,
			Labels: common.MergeLabels(
				c.Spec.ClickHouse.Labels,
				map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
			),
			Annotations: annotationMap,
		},
		Spec: v1.ServiceSpec{
			Ports:    svcPorts,
			Selector: map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
		},
	}
	service.Spec.ClusterIP = v1.ClusterIPNone
	return &service
}

func MakeChStatefulSet(c *v1beta1.ClickHouseCluster, shard, replica int32) *appsv1.StatefulSet {
	var chDataVolume = "data"
	var one = int32(1)
	var extraVolumes []v1.Volume
	persistence := v1beta1.Persistence{}
	persistence.WithDefaults()
	var pvcs []v1.PersistentVolumeClaim
	if strings.EqualFold(c.Spec.ClickHouse.StorageType, "ephemeral") {
		extraVolumes = append(extraVolumes, v1.Volume{
			Name: chDataVolume,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		})
	} else {
		pvcs = append(pvcs, v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: chDataVolume,
				Labels: common.MergeLabels(
					c.Spec.ClickHouse.Labels,
					map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
				),
				Annotations: c.Spec.ClickHouse.Persistence.Annotations,
			},
			Spec: persistence.PersistentVolumeClaimSpec,
		})
	}
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.GetChStatefulSetName(shard, replica),
			Namespace: c.Namespace,
			Labels: common.MergeLabels(
				c.Spec.ClickHouse.Labels,
				map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
			),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: c.GetChStatefulSetName(shard, replica),
			Replicas:    &one,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: c.GetChStatefulSetName(shard, replica),
					Labels: common.MergeLabels(
						c.Spec.ClickHouse.Labels,
						map[string]string{"app": c.GetChStatefulSetName(shard, replica)},
					),
				},
				Spec: makeChPodSpec(c, extraVolumes, shard, replica),
			},
			VolumeClaimTemplates: pvcs,
		},
	}
}

func makeChPodSpec(c *v1beta1.ClickHouseCluster, volumes []v1.Volume, shard, replica int32) v1.PodSpec {
	chInitContainer := v1.Container{
		Name:            "init-container",
		Image:           "busybox:1.28.3",
		ImagePullPolicy: c.Spec.ClickHouse.Image.PullPolicy,
		Command:         []string{"sh", "-c", config.GenerateChInitStartCommand(c)},
	}

	ports := v1beta1.FillChContainerPortsWithDefaults(c.Spec.Zookeeper.Ports)
	image := v1beta1.ContainerImage{}
	image.WithChDefaults()
	chContainer := v1.Container{
		Name:            "clickhouse-server",
		Image:           image.ToString(),
		Ports:           ports,
		ImagePullPolicy: image.PullPolicy,
		ReadinessProbe: &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.IntOrString{
						IntVal: 8123,
					},
				},
			},
			InitialDelaySeconds: 10,
			TimeoutSeconds:      10,
			PeriodSeconds:       30,
			SuccessThreshold:    1,
			FailureThreshold:    5,
		},
		LivenessProbe: &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.IntOrString{
						IntVal: 8123,
					},
				},
			},
			InitialDelaySeconds: 300,
			TimeoutSeconds:      30,
			PeriodSeconds:       60,
			SuccessThreshold:    1,
			FailureThreshold:    5,
		},
		VolumeMounts: []v1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/clickhouse"},
			{Name: "config", MountPath: "/etc/clickhouse-server/config.d"},
			{Name: "private-config", MountPath: "/etc/clickhouse-server/conf.d"},
		},
		Command: []string{"bash", "-c", config.GenerateChStartCommand()},
	}

	chContainer.Resources = c.Spec.ClickHouse.Resources
	volumes = append(volumes, []v1.Volume{
		{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.GetChName(),
					},
				},
			},
		},
		{
			Name: "private-config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.GetChStatefulSetName(shard, replica),
					},
				},
			},
		},
	}...)

	metricsContainer := v1.Container{
		Name:            "clickhouse-exporter",
		Image:           "f1yegor/clickhouse-exporter",
		Ports:           []v1.ContainerPort{v1.ContainerPort{ContainerPort: 9116}},
		ImagePullPolicy: image.PullPolicy,
		Command: []string{"sh", "-c",
			"/usr/local/bin/clickhouse_exporter -scrape_uri=http://localhost:8123"},
	}

	podSpec := v1.PodSpec{
		InitContainers: []v1.Container{chInitContainer},
		Containers:     []v1.Container{chContainer, metricsContainer},
		Volumes:        volumes,
	}
	podSpec.HostAliases = []v1.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{c.GetChStatefulSetName(shard, replica)},
		},
	}
	return podSpec
}

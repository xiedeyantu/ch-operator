package clickhousecluster

import (
	"fmt"
	"github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	"github.com/xiedeyantu/ch-operator/pkg/config"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"reflect"
	"strconv"
	"strings"
)

func MakeChConfigMap(c *v1beta1.ClickHouseCluster) *v1.ConfigMap {
	var zkDomain []string
	for i := int32(0); i < c.Spec.Zookeeper.Replicas; i++ {
		zkDomain = append(zkDomain, GetZkDomainString(c, i))
	}
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.Spec.ClickHouse.Name,
			Namespace: c.Namespace,
			Labels:    c.Spec.Zookeeper.Labels,
		},
		Data: map[string]string{
			"zookeeper.xml":   config.GenerateChZkConfig(zkDomain),
			"host-listen.xml": config.GenerateChListenConfig(),
			"remote_servers.xml": config.GenerateChRemoteConfig(c.Spec.ClickHouse.Shards,
				c.Spec.ClickHouse.Replicas,
				c.Spec.ClickHouse.Name,
				c.GetNamespace(),
				c.GetKubernetesClusterDomain()),
		},
	}
}

func MakeChPrivateConfigMap(c *v1beta1.ClickHouseCluster, shard, replica int32) *v1.ConfigMap {
	var zkDomain []string
	for i := int32(0); i < c.Spec.Zookeeper.Replicas; i++ {
		zkDomain = append(zkDomain, GetZkDomainString(c, i))
	}
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateConfigMapName(c.Spec.ClickHouse.Name, shard, replica),
			Namespace: c.Namespace,
			Labels:    c.Spec.ClickHouse.Labels,
		},
		Data: map[string]string{
			"macros.xml": config.GenerateChMacrosConfig(shard, replica, c.Spec.ClickHouse.Name),
		},
	}
}

func generateConfigMapName(name string, shard, replica int32) (cmName string) {
	return fmt.Sprintf("%s-private-%s-%s",
		name,
		strconv.FormatInt(int64(shard), 10),
		strconv.FormatInt(int64(replica), 10))
}

func MakeChHeadlessService(c *v1beta1.ClickHouseCluster, shard, replica int32) *v1.Service {
	svcPorts := []v1.ServicePort{
		{
			Name: "tcp",
			Port: 9000,
		},
		{
			Name: "http",
			Port: 8123,
		},
		{
			Name: "inner",
			Port: 9009,
		},
	}
	return makeChService(config.ChHeadlessSvcName(c.Spec.ClickHouse.Name, shard, replica), svcPorts, false, c, shard, replica)
}

func makeChService(name string, ports []v1.ServicePort, clusterIP bool, c *v1beta1.ClickHouseCluster, shard, replica int32) *v1.Service {
	var dnsName string
	var annotationMap map[string]string
	if !clusterIP && c.Spec.ClickHouse.DomainName != "" {
		domainName := strings.TrimSpace(c.Spec.Zookeeper.DomainName)
		if strings.HasSuffix(domainName, dot) {
			dnsName = name + dot + domainName
		} else {
			dnsName = name + dot + domainName + dot
		}
		annotationMap = map[string]string{externalDNSAnnotationKey: dnsName}
	} else {
		annotationMap = map[string]string{}
	}
	service := v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels: mergeLabels(
				c.Spec.ClickHouse.Labels,
				map[string]string{"app": c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10)},
			),
			Annotations: annotationMap,
		},
		Spec: v1.ServiceSpec{
			Ports:    ports,
			Selector: map[string]string{"app": name},
		},
	}
	if !clusterIP {
		service.Spec.ClusterIP = v1.ClusterIPNone
	}
	return &service
}

var chDataVolume = "data"
var one = int32(1)

func MakeChStatefulSet(c *v1beta1.ClickHouseCluster, shard, replica int32) *appsv1.StatefulSet {
	var extraVolumes []v1.Volume
	persistence := c.Spec.ClickHouse.Persistence
	var pvcs []v1.PersistentVolumeClaim
	if strings.EqualFold(c.Spec.ClickHouse.StorageType, "ephemeral") {
		extraVolumes = append(extraVolumes, v1.Volume{
			Name: chDataVolume,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &c.Spec.ClickHouse.Ephemeral.EmptyDirVolumeSource,
			},
		})
	} else {
		persistence.PersistentVolumeClaimSpec.Resources.Requests = v1.ResourceList{
			v1.ResourceStorage: resource.MustParse("20Gi"),
		}
		persistence.PersistentVolumeClaimSpec.AccessModes = []v1.PersistentVolumeAccessMode{
			v1.ReadWriteOnce,
		}
		pvcs = append(pvcs, v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: chDataVolume,
				Labels: mergeLabels(
					c.Spec.ClickHouse.Labels,
					map[string]string{"app": config.ChHeadlessSvcName(c.Spec.ClickHouse.Name, shard, replica)},
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
			Name:      c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10),
			Namespace: c.Namespace,
			Labels: mergeLabels(
				c.Spec.ClickHouse.Labels,
				map[string]string{"app": c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10)},
			),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "ch",
			Replicas:    &one,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10),
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: c.Spec.ClickHouse.Name,
					Labels: mergeLabels(
						c.Spec.ClickHouse.Labels,
						map[string]string{
							"app": c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10),
						},
					),
					Annotations: c.Spec.ClickHouse.Pod.Annotations,
				},
				Spec: makeChPodSpec(c, extraVolumes, shard, replica),
			},
			VolumeClaimTemplates: pvcs,
		},
	}
}

func makeChPodSpec(c *v1beta1.ClickHouseCluster, volumes []v1.Volume, shard, replica int32) v1.PodSpec {
	chInitContainer := v1.Container{
		Name:            "wait",
		Image:           "busybox:1.28.3",
		ImagePullPolicy: c.Spec.ClickHouse.Image.PullPolicy,
		Command:         []string{"sh", "-c", makeChInitStartCommand()},
	}

	if c.Spec.ClickHouse.Ports == nil {
		c.Spec.ClickHouse.Ports = []v1.ContainerPort{
			{
				Name:          "tcp",
				ContainerPort: 9000,
			},
			{
				Name:          "http",
				ContainerPort: 8123,
			},
			{
				Name:          "inner",
				ContainerPort: 9009,
			},
		}
	}

	chContainer := v1.Container{
		Name:            "chserver",
		Image:           c.Spec.ClickHouse.Image.ToString(),
		Ports:           c.Spec.ClickHouse.Ports,
		ImagePullPolicy: c.Spec.ClickHouse.Image.PullPolicy,
		//ReadinessProbe: &v1.Probe{
		//	Handler: v1.Handler{
		//		Exec: &v1.ExecAction{Command: []string{"bash", "-c", "OK=$(echo ruok | nc 127.0.0.1 2181); if [[ \"$OK\" == \"imok\" ]]; then exit 0; else exit 1; fi"}},
		//	},
		//	InitialDelaySeconds: 10,
		//	TimeoutSeconds:      10,
		//	PeriodSeconds:       10,
		//	SuccessThreshold:    1,
		//	FailureThreshold:    5,
		//},
		LivenessProbe: &v1.Probe{
			Handler: v1.Handler{
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
		Command: []string{"bash", "-c", makeChStartCommand()},
	}
	if c.Spec.ClickHouse.Pod.Resources.Limits != nil || c.Spec.ClickHouse.Pod.Resources.Requests != nil {
		chContainer.Resources = c.Spec.ClickHouse.Pod.Resources
	}
	volumes = append(volumes, v1.Volume{
		Name: "config",
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: c.Spec.ClickHouse.Name,
				},
			},
		},
	})
	volumes = append(volumes, v1.Volume{
		Name: "private-config",
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: c.Spec.ClickHouse.Name + "-private-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10),
				},
			},
		},
	})

	podSpec := v1.PodSpec{
		InitContainers: []v1.Container{chInitContainer},
		Containers:     append(c.Spec.ClickHouse.Containers, chContainer),
		Affinity:       c.Spec.ClickHouse.Pod.Affinity,
		Volumes:        append(c.Spec.ClickHouse.Volumes, volumes...),
	}
	if reflect.DeepEqual(v1.PodSecurityContext{}, c.Spec.ClickHouse.Pod.SecurityContext) {
		podSpec.SecurityContext = c.Spec.ClickHouse.Pod.SecurityContext
	}
	podSpec.NodeSelector = c.Spec.ClickHouse.Pod.NodeSelector
	podSpec.Tolerations = c.Spec.ClickHouse.Pod.Tolerations
	podSpec.TerminationGracePeriodSeconds = &c.Spec.ClickHouse.Pod.TerminationGracePeriodSeconds
	podSpec.ServiceAccountName = c.Spec.ClickHouse.Pod.ServiceAccountName
	podSpec.HostAliases = []v1.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{c.Spec.ClickHouse.Name + "-" + strconv.FormatInt(int64(shard), 10) + "-" + strconv.FormatInt(int64(replica), 10)},
		},
	}
	return podSpec
}

func makeChInitStartCommand() string {
	return `
  while true; do
	for i in $(seq 1 3); do
	  domain=zookeeper-$(($i-1)).zookeeper-headless.default.svc.cluster.local
	  role=$(echo srvr | nc $domain 2181|grep Mode|awk '{print $2}');
	  echo $domain $role
	  if [[ $role = "leader" ]]; then
		exit 0;
	  fi
	done;
  done;`
}

func makeChStartCommand() string {
	return "/entrypoint.sh"
}

//func makeChStartCommand() string {
//	return "
//mkdir -p /etc/clickhouse-server/conf.d &&
//{
//echo '<yandex>'
//echo '  <macros>'
//echo '    <cluster>default</cluster>'
//echo '    <shard>0</shard>'
//echo "    <replica>${HOSTNAME}</replica>"
//echo '  </macros>'
//echo '</yandex>'
//} > /etc/clickhouse-server/conf.d/macros.xml &&
//{
//echo '<yandex>'
//echo '  <listen_host>::</listen_host>'
//echo '  <listen_host>0.0.0.0</listen_host>'
//echo '  <listen_try>1</listen_try>'
//echo '</yandex>'
//} > /etc/clickhouse-server/conf.d/listen_host.xml &&
///entrypoint.sh`
//}

package clickhousecluster

import (
	"fmt"
	"github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"reflect"
	"strconv"
	"strings"
)

const (
	externalDNSAnnotationKey = "external-dns.alpha.kubernetes.io/hostname"
	dot                      = "."
)

func headlessDomain(c *v1beta1.ClickHouseCluster) string {
	return fmt.Sprintf("%s.%s.svc.%s", zkHeadlessSvcName(c), c.GetNamespace(), c.GetKubernetesClusterDomain())
}

func zkHeadlessSvcName(c *v1beta1.ClickHouseCluster) string {
	return fmt.Sprintf("%s-headless", c.Spec.Zookeeper.Name)
}

var zkDataVolume = "data"

// MakeStatefulSet return a zookeeper stateful set from the zk spec
func MakeStatefulSet(c *v1beta1.ClickHouseCluster) *appsv1.StatefulSet {
	extraVolumes := []v1.Volume{}
	persistence := c.Spec.Zookeeper.Persistence
	pvcs := []v1.PersistentVolumeClaim{}
	if strings.EqualFold(c.Spec.Zookeeper.StorageType, "ephemeral") {
		extraVolumes = append(extraVolumes, v1.Volume{
			Name: zkDataVolume,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &c.Spec.Zookeeper.Ephemeral.EmptyDirVolumeSource,
			},
		})
	} else {
		pvcs = append(pvcs, v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: zkDataVolume,
				Labels: mergeLabels(
					c.Spec.Zookeeper.Labels,
					map[string]string{"app": c.Name},
				),
				Annotations: c.Spec.Zookeeper.Persistence.Annotations,
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
			Name:      c.Spec.Zookeeper.Name,
			Namespace: c.Namespace,
			Labels:    c.Spec.Zookeeper.Labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: zkHeadlessSvcName(c),
			Replicas:    &c.Spec.Zookeeper.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": c.Spec.Zookeeper.Name,
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: c.Spec.Zookeeper.Name,
					Labels: mergeLabels(
						c.Spec.Zookeeper.Labels,
						map[string]string{
							"app": c.Spec.Zookeeper.Name,
						},
					),
					Annotations: c.Spec.Zookeeper.Pod.Annotations,
				},
				Spec: makeZkPodSpec(c, extraVolumes),
			},
			VolumeClaimTemplates: pvcs,
		},
	}
}

func makeZkPodSpec(z *v1beta1.ClickHouseCluster, volumes []v1.Volume) v1.PodSpec {
	zkContainer := v1.Container{
		Name:            "zookeeper",
		Image:           z.Spec.Zookeeper.Image.ToString(),
		Ports:           z.Spec.Zookeeper.Ports,
		ImagePullPolicy: z.Spec.Zookeeper.Image.PullPolicy,
		ReadinessProbe: &v1.Probe{
			Handler: v1.Handler{
				Exec: &v1.ExecAction{Command: []string{"bash", "-c", "OK=$(echo ruok | nc 127.0.0.1 2181); if [[ \"$OK\" == \"imok\" ]]; then exit 0; else exit 1; fi"}},
			},
			InitialDelaySeconds: 10,
			TimeoutSeconds:      30,
			PeriodSeconds:       30,
			SuccessThreshold:    1,
			FailureThreshold:    5,
		},
		LivenessProbe: &v1.Probe{
			Handler: v1.Handler{
				Exec: &v1.ExecAction{Command: []string{"bash", "-c", "OK=$( echo srvr | nc 127.0.0.1 2181|grep Mode|awk '{print $2}'); if [[ \"$OK\" == \"follower\" || \"$OK\" == \"leader\" ]]; then exit 0; else exit 1; fi"}},
			},
			InitialDelaySeconds: 10,
			TimeoutSeconds:      30,
			PeriodSeconds:       30,
			SuccessThreshold:    1,
			FailureThreshold:    5,
		},
		VolumeMounts: []v1.VolumeMount{
			{Name: "data", MountPath: "/data"},
			{Name: "conf", MountPath: "/conf"},
		},
		Command: []string{"bash", "-c", makeZkStartCommand()},
	}
	if z.Spec.Zookeeper.Pod.Resources.Limits != nil || z.Spec.Zookeeper.Pod.Resources.Requests != nil {
		zkContainer.Resources = z.Spec.Zookeeper.Pod.Resources
	}
	volumes = append(volumes, v1.Volume{
		Name: "conf",
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: z.ConfigMapName(),
				},
			},
		},
	})

	podSpec := v1.PodSpec{
		Containers: append(z.Spec.Zookeeper.Containers, zkContainer),
		Affinity:   z.Spec.Zookeeper.Pod.Affinity,
		Volumes:    append(z.Spec.Zookeeper.Volumes, volumes...),
	}
	if reflect.DeepEqual(v1.PodSecurityContext{}, z.Spec.Zookeeper.Pod.SecurityContext) {
		podSpec.SecurityContext = z.Spec.Zookeeper.Pod.SecurityContext
	}
	podSpec.NodeSelector = z.Spec.Zookeeper.Pod.NodeSelector
	podSpec.Tolerations = z.Spec.Zookeeper.Pod.Tolerations
	podSpec.TerminationGracePeriodSeconds = &z.Spec.Zookeeper.Pod.TerminationGracePeriodSeconds
	podSpec.ServiceAccountName = z.Spec.Zookeeper.Pod.ServiceAccountName
	return podSpec
}

func MakeZkConfigMap(c *v1beta1.ClickHouseCluster) *v1.ConfigMap {
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.ConfigMapName(),
			Namespace: c.Namespace,
			Labels:    c.Spec.Zookeeper.Labels,
		},
		Data: map[string]string{
			"zoo.cfg":          makeZkConfigString(c),
			"log4j.properties": makeZkLog4JConfigString(),
		},
	}
}

// MakeHeadlessService returns an internal headless-service for the zk
// stateful-set
func MakeHeadlessService(c *v1beta1.ClickHouseCluster) *v1.Service {
	ports := c.ZookeeperPorts()
	svcPorts := []v1.ServicePort{
		{Name: "tcp-client", Port: ports.Client},
		{Name: "tcp-quorum", Port: ports.Quorum},
		{Name: "tcp-leader-election", Port: ports.Leader},
		{Name: "tcp-metrics", Port: ports.Metrics},
	}
	return makeZkService(zkHeadlessSvcName(c), svcPorts, false, c)
}

func makeZkStartCommand() string {
	return `
SERVERS=3 &&
{
if [ ! -f "$ZOO_DATA_DIR/myid" ];then
  sleep 30
fi
} &&
HOST=$(hostname -s) &&
if [[ $HOST =~ (.*)-([0-9]+)$ ]]; then
  NAME=${BASH_REMATCH[1]}
  ORD=${BASH_REMATCH[2]}
else
  echo "Failed to parse name and ordinal of Pod"
  exit 1
fi &&
export MY_ID=$((ORD+1)) &&
echo $MY_ID > $ZOO_DATA_DIR/myid &&
cat /conf/zoo.cfg && 
zkServer.sh start-foreground`
}

func makeZkConfigString(c *v1beta1.ClickHouseCluster) string {
	s := c.Spec.Zookeeper
	serverConf := ""
	for i := int32(0); i < s.Replicas; i++ {
		serverConf = serverConf + fmt.Sprintf("server.%d=%s:2888:3888\n", i+1, GetZkDomainString(c, i))
	}
	return "4lw.commands.whitelist=cons, envi, conf, crst, srvr, stat, mntr, ruok\n" +
		"clientPort=2181\n" +
		"dataDir=/data\n" +
		"skipACL=yes\n" +
		"metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider\n" +
		"metricsProvider.httpPort=7000\n" +
		"metricsProvider.exportJvmInfo=true\n" +
		"initLimit=" + strconv.Itoa(s.Conf.InitLimit) + "\n" +
		"syncLimit=" + strconv.Itoa(s.Conf.SyncLimit) + "\n" +
		"tickTime=" + strconv.Itoa(s.Conf.TickTime) + "\n" +
		"globalOutstandingLimit=" + strconv.Itoa(s.Conf.GlobalOutstandingLimit) + "\n" +
		"preAllocSize=" + strconv.Itoa(s.Conf.PreAllocSize) + "\n" +
		"snapCount=" + strconv.Itoa(s.Conf.SnapCount) + "\n" +
		"commitLogCount=" + strconv.Itoa(s.Conf.CommitLogCount) + "\n" +
		"snapSizeLimitInKb=" + strconv.Itoa(s.Conf.SnapSizeLimitInKb) + "\n" +
		"maxCnxns=" + strconv.Itoa(s.Conf.MaxCnxns) + "\n" +
		"maxClientCnxns=" + strconv.Itoa(s.Conf.MaxClientCnxns) + "\n" +
		"minSessionTimeout=" + strconv.Itoa(s.Conf.MinSessionTimeout) + "\n" +
		"maxSessionTimeout=" + strconv.Itoa(s.Conf.MaxSessionTimeout) + "\n" +
		"autopurge.snapRetainCount=" + strconv.Itoa(s.Conf.AutoPurgeSnapRetainCount) + "\n" +
		"autopurge.purgeInterval=" + strconv.Itoa(s.Conf.AutoPurgePurgeInterval) + "\n" +
		"quorumListenOnAllIPs=" + strconv.FormatBool(s.Conf.QuorumListenOnAllIPs) + "\n" +
		serverConf
}

func makeZkLog4JConfigString() string {
	return "zookeeper.root.logger=CONSOLE\n" +
		"zookeeper.console.threshold=INFO\n" +
		"log4j.rootLogger=${zookeeper.root.logger}\n" +
		"log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
		"log4j.appender.CONSOLE.Threshold=${zookeeper.console.threshold}\n" +
		"log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
		"log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n\n"
}

func makeZkService(name string, ports []v1.ServicePort, clusterIP bool, c *v1beta1.ClickHouseCluster) *v1.Service {
	var dnsName string
	var annotationMap map[string]string
	if !clusterIP && c.Spec.Zookeeper.DomainName != "" {
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
				c.Spec.Zookeeper.Labels,
				map[string]string{"app": c.Spec.Zookeeper.Name, "headless": strconv.FormatBool(!clusterIP)},
			),
			Annotations: annotationMap,
		},
		Spec: v1.ServiceSpec{
			Ports:    ports,
			Selector: map[string]string{"app": c.Spec.Zookeeper.Name},
		},
	}
	if !clusterIP {
		service.Spec.ClusterIP = v1.ClusterIPNone
	}
	return &service
}

// MakePodDisruptionBudget returns a pdb for the zookeeper cluster
func MakePodDisruptionBudget(z *v1beta1.ClickHouseCluster) *policyv1beta1.PodDisruptionBudget {
	pdbCount := intstr.FromInt(1)
	return &policyv1beta1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      z.GetName(),
			Namespace: z.Namespace,
			Labels:    z.Spec.Zookeeper.Labels,
		},
		Spec: policyv1beta1.PodDisruptionBudgetSpec{
			MaxUnavailable: &pdbCount,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": z.GetName(),
				},
			},
		},
	}
}

// MergeLabels merges label maps
func mergeLabels(l ...map[string]string) map[string]string {
	res := make(map[string]string)

	for _, v := range l {
		for lKey, lValue := range v {
			res[lKey] = lValue
		}
	}
	return res
}

func GetZkDomainString(c *v1beta1.ClickHouseCluster, index int32) (ZkDomain string) {
	ZkDomain = fmt.Sprintf("%s-%d.%s.%s.svc.%s",
		c.Spec.Zookeeper.Name,
		index,
		zkHeadlessSvcName(c),
		c.GetNamespace(),
		c.GetKubernetesClusterDomain())
	return ZkDomain
}

// SyncService synchronizes a service with an updated spec and validates it
func SyncService(curr *v1.Service, next *v1.Service) {
	curr.Spec.Ports = next.Spec.Ports
	curr.Spec.Type = next.Spec.Type
}

// SyncConfigMap synchronizes a configmap with an updated spec and validates it
func SyncConfigMap(curr *v1.ConfigMap, next *v1.ConfigMap) {
	curr.Data = next.Data
	curr.BinaryData = next.BinaryData
}

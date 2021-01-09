package clickhousecluster

import (
	"fmt"
	"github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	"github.com/xiedeyantu/ch-operator/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"
	"strings"
)

const (
	externalDNSAnnotationKey = "external-dns.alpha.kubernetes.io/hostname"
	dot                      = "."
)

// MakeStatefulSet return a zookeeper stateful set from the zk spec
func MakeStatefulSet(c *v1beta1.ClickHouseCluster) *appsv1.StatefulSet {
	var zkDataVolume = "data"
	var extraVolumes []v1.Volume
	persistence := v1beta1.Persistence{}
	persistence.WithDefaults()
	var pvcs []v1.PersistentVolumeClaim
	if strings.EqualFold(c.Spec.Zookeeper.StorageType, "ephemeral") {
		extraVolumes = append(extraVolumes, v1.Volume{
			Name: zkDataVolume,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{
					Medium:    "",
					SizeLimit: nil,
				},
			},
		})
	} else {
		pvcs = append(pvcs, v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: zkDataVolume,
				Labels: common.MergeLabels(
					c.Spec.Zookeeper.Labels,
					map[string]string{"app": c.GetZkName()},
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
			Name:      c.GetZkName(),
			Namespace: c.Namespace,
			Labels: common.MergeLabels(
				c.Spec.Zookeeper.Labels,
				map[string]string{"app": c.GetZkName()},
			),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: fmt.Sprintf("%s-zk", c.GetZkName()),
			Replicas:    &c.Spec.Zookeeper.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": c.GetZkName(),
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: fmt.Sprintf("%s-zk", c.GetZkName()),
					Labels: common.MergeLabels(
						c.Spec.Zookeeper.Labels,
						map[string]string{"app": c.GetZkName()},
					),
					Annotations: c.Spec.Zookeeper.Pod.Annotations,
				},
				Spec: makeZkPodSpec(c, extraVolumes),
			},
			VolumeClaimTemplates: pvcs,
		},
	}
}

func makeZkPodSpec(c *v1beta1.ClickHouseCluster, volumes []v1.Volume) v1.PodSpec {
	ports := v1beta1.FillZkContainerPortsWithDefaults(c.Spec.Zookeeper.Ports)
	image := v1beta1.ContainerImage{}
	image.WithZkDefaults()
	zkContainer := v1.Container{
		Name:            "zookeeper-server",
		Image:           image.ToString(),
		Ports:           ports,
		ImagePullPolicy: image.PullPolicy,
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
	if c.Spec.Zookeeper.Pod.Resources.Limits != nil || c.Spec.Zookeeper.Pod.Resources.Requests != nil {
		zkContainer.Resources = c.Spec.Zookeeper.Pod.Resources
	}
	volumes = append(volumes, v1.Volume{
		Name: "conf",
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: c.ConfigMapName(),
				},
			},
		},
	})
	podPolicy := v1beta1.PodPolicy{}
	podPolicy.WithDefaults(c)
	podSpec := v1.PodSpec{
		Containers: append(c.Spec.Zookeeper.Containers, zkContainer),
		Affinity:   podPolicy.Affinity,
		Volumes:    append(c.Spec.Zookeeper.Volumes, volumes...),
	}
	//if reflect.DeepEqual(v1.PodSecurityContext{}, c.Spec.Zookeeper.Pod.SecurityContext) {
	//	podSpec.SecurityContext = c.Spec.Zookeeper.Pod.SecurityContext
	//}
	podSpec.NodeSelector = c.Spec.Zookeeper.Pod.NodeSelector
	podSpec.Tolerations = c.Spec.Zookeeper.Pod.Tolerations
	podSpec.TerminationGracePeriodSeconds = &c.Spec.Zookeeper.Pod.TerminationGracePeriodSeconds
	podSpec.ServiceAccountName = c.Spec.Zookeeper.Pod.ServiceAccountName
	return podSpec
}

func MakeZkConfigMap(c *v1beta1.ClickHouseCluster) *v1.ConfigMap {
	conf := &c.Spec.Zookeeper.Conf
	conf.WithDefaults()
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.ConfigMapName(),
			Namespace: c.Namespace,
			Labels: common.MergeLabels(
				c.Spec.Zookeeper.Labels,
				map[string]string{"app": c.GetZkName()},
			),
		},
		Data: map[string]string{
			"zoo.cfg":          makeZkConfigString(c),
			"log4j.properties": makeZkLog4JConfigString(),
		},
	}
}

// MakeZkHeadlessService returns an internal headless-service for the zk
// stateful-set
func MakeZkHeadlessService(c *v1beta1.ClickHouseCluster) *v1.Service {
	var dnsName string
	var annotationMap map[string]string
	svcName := fmt.Sprintf("%s-zk", c.GetZkName())
	if c.Spec.Zookeeper.DomainName != "" {
		domainName := strings.TrimSpace(c.Spec.Zookeeper.DomainName)
		if strings.HasSuffix(domainName, dot) {
			dnsName = svcName + dot + domainName
		} else {
			dnsName = svcName + dot + domainName + dot
		}
		annotationMap = map[string]string{externalDNSAnnotationKey: dnsName}
	} else {
		annotationMap = map[string]string{}
	}

	svcPorts := v1beta1.FillZkServicePortsWithDefaults(c.Spec.Zookeeper.Ports)
	service := v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: c.Namespace,
			Labels: common.MergeLabels(
				c.Spec.Zookeeper.Labels,
				map[string]string{"app": c.GetZkName()},
			),
			Annotations: annotationMap,
		},
		Spec: v1.ServiceSpec{
			Ports:     svcPorts,
			Selector:  map[string]string{"app": c.GetZkName()},
			ClusterIP: v1.ClusterIPNone,
		},
	}

	return &service
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
		serverConf = serverConf + fmt.Sprintf("server.%d=%s:2888:3888\n", i+1, v1beta1.GenerateZkDomain(c, i))
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

package clickhousecluster

import (
	"context"
	"github.com/go-logr/logr"
	clickhousev1beta1 "github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	"github.com/xiedeyantu/ch-operator/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

// ReconcileTime is the delay between reconciliations
//const ReconcileTime = 60 * time.Second

var log = logf.Log.WithName("controller_clickhousecluster")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new ClickHouseCluster Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileClickHouseCluster{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("clickhousecluster-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource ClickHouseCluster
	err = c.Watch(&source.Kind{Type: &clickhousev1beta1.ClickHouseCluster{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner ClickHouseCluster
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &clickhousev1beta1.ClickHouseCluster{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileClickHouseCluster implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileClickHouseCluster{}

// ReconcileClickHouseCluster reconciles a ClickHouseCluster object
type ReconcileClickHouseCluster struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	log    logr.Logger
}

type reconcileFun func(cluster *clickhousev1beta1.ClickHouseCluster) error

// Reconcile reads that state of the cluster for a ClickHouseCluster object and makes changes based on the state read
// and what is in the ClickHouseCluster.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileClickHouseCluster) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	r.log = log.WithValues(
		"Request.Namespace", request.Namespace,
		"Request.Name", request.Name)
	r.log.Info("Reconciling ClickHouseCluster")

	// Fetch the ClickHouseCluster instance
	instance := &clickhousev1beta1.ClickHouseCluster{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	changed := instance.WithDefaults()
	if changed {
		r.log.Info("Setting default settings for clickhouse-cluster")
		if err := r.client.Update(context.TODO(), instance); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}
	for _, fun := range []reconcileFun{
		//r.reconcileFinalizers,
		r.reconcileZkConfigMap,
		r.reconcileZkHeadlessService,
		r.reconcileZkStatefulSet,
		r.reconcileZkPodDisruptionBudget,

		r.reconcileZkClusterStatus,
	} {
		if err = fun(instance); err != nil {
			return reconcile.Result{}, err
		}
	}

	r.waitZkStatefulSetFinish(instance)

	for _, fun := range []reconcileFun{
		//r.reconcileFinalizers,
		r.reconcileChConfigMap,
		r.reconcileChHeadlessService,
		r.reconcileChStatefulSet,

		//r.reconcileZkClusterStatus,
	} {
		if err = fun(instance); err != nil {
			return reconcile.Result{}, err
		}
	}

	r.waitChStatefulSetFinish(instance)

	// Recreate any missing resources every 'ReconcileTime'
	//return reconcile.Result{RequeueAfter: ReconcileTime}, nil
	return reconcile.Result{}, nil
}

func (r *ReconcileClickHouseCluster) CreateOrUpdateChConfigMap(instance *clickhousev1beta1.ClickHouseCluster, configMapType string) (err error) {
	var configMapList []*corev1.ConfigMap
	if configMapType == common.Common {
		configMapList = append(configMapList, MakeChConfigMap(instance))
	} else {
		for i := int32(0); i < instance.Spec.ClickHouse.Shards; i++ {
			for j := int32(0); j < instance.Spec.ClickHouse.Replicas; j++ {
				configMapList = append(configMapList, MakeChPrivateConfigMap(instance, i, j))
			}
		}
	}

	for _, cm := range configMapList {
		if err = controllerutil.SetControllerReference(instance, cm, r.scheme); err != nil {
			return err
		}
		foundCm := &corev1.ConfigMap{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      cm.Name,
			Namespace: cm.Namespace,
		}, foundCm)
		if err != nil && errors.IsNotFound(err) {
			r.log.Info("Creating a new ClickHouse ConfigMap",
				"ConfigMap.Namespace", cm.Namespace,
				"ConfigMap.Name", cm.Name)
			err = r.client.Create(context.TODO(), cm)
			if err != nil {
				return err
			}
			//return nil
		} else if err != nil {
			return err
		} else {
			r.log.Info("Updating existing ConfigMap",
				"ConfigMap.Namespace", foundCm.Namespace,
				"ConfigMap.Name", foundCm.Name)
			SyncConfigMap(foundCm, cm)
			err = r.client.Update(context.TODO(), foundCm)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *ReconcileClickHouseCluster) reconcileChConfigMap(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	err = r.CreateOrUpdateChConfigMap(instance, common.Common)
	if err != nil {
		return err
	}
	err = r.CreateOrUpdateChConfigMap(instance, common.Private)
	if err != nil {
		return err
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileChStatefulSet(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	for shard := int32(0); shard < instance.Spec.ClickHouse.Shards; shard++ {
		for replica := int32(0); replica < instance.Spec.ClickHouse.Replicas; replica++ {
			err = r.CreateOrUpdateChStatefulSet(instance, shard, replica)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ReconcileClickHouseCluster) CreateOrUpdateChStatefulSet(instance *clickhousev1beta1.ClickHouseCluster, shard, replica int32) (err error) {
	sts := MakeChStatefulSet(instance, shard, replica)
	if err = controllerutil.SetControllerReference(instance, sts, r.scheme); err != nil {
		return err
	}
	foundSts := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	}, foundSts)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating a new ClickHouse StatefulSet",
			"StatefulSet.Namespace", sts.Namespace,
			"StatefulSet.Name", sts.Name)
		err = r.client.Create(context.TODO(), sts)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkConfigMap(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	cm := MakeZkConfigMap(instance)
	if err = controllerutil.SetControllerReference(instance, cm, r.scheme); err != nil {
		return err
	}
	foundCm := &corev1.ConfigMap{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      cm.Name,
		Namespace: cm.Namespace,
	}, foundCm)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating a new Zookeeper Config Map",
			"ConfigMap.Namespace", cm.Namespace,
			"ConfigMap.Name", cm.Name)
		err = r.client.Create(context.TODO(), cm)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.log.Info("Updating existing config-map",
			"ConfigMap.Namespace", foundCm.Namespace,
			"ConfigMap.Name", foundCm.Name)
		SyncConfigMap(foundCm, cm)
		err = r.client.Update(context.TODO(), foundCm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkStatefulSet(instance *clickhousev1beta1.ClickHouseCluster) (err error) {

	// we cannot upgrade if cluster is in UpgradeFailed
	if instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}
	sts := MakeStatefulSet(instance)
	if err = controllerutil.SetControllerReference(instance, sts, r.scheme); err != nil {
		return err
	}
	foundSts := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	}, foundSts)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating a new Zookeeper StatefulSet",
			"StatefulSet.Namespace", sts.Namespace,
			"StatefulSet.Name", sts.Name)
		err = r.client.Create(context.TODO(), sts)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		return nil
		//foundSTSSize := *foundSts.Spec.Replicas
		//newSTSSize := *sts.Spec.Replicas
		//if newSTSSize != foundSTSSize {
		//	zkUri := common.GetZkServiceUri(instance)
		//	err = r.zkClient.Connect(zkUri)
		//	if err != nil {
		//		return fmt.Errorf("Error storing cluster size %v", err)
		//	}
		//	defer r.zkClient.Close()
		//	r.log.Info("Connected to ZK", "ZKURI", zkUri)
		//
		//	path := common.GetMetaPath(instance)
		//	version, err := r.zkClient.NodeExists(path)
		//	if err != nil {
		//		return fmt.Errorf("Error doing exists check for znode %s: %v", path, err)
		//	}
		//
		//	data := "CLUSTER_SIZE=" + strconv.Itoa(int(newSTSSize))
		//	r.log.Info("Updating Cluster Size.", "New Data:", data, "Version", version)
		//	r.zkClient.UpdateNode(path, data, version)
		//}
		//err = r.updateStatefulSet(instance, foundSts, sts)
		//if err != nil {
		//	return err
		//}
		//return r.upgradeStatefulSet(instance, foundSts)
	}
}

func (r *ReconcileClickHouseCluster) waitChStatefulSetFinish(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	r.log.Info("reconcile clickhouse pods start...")

	for {
		foundSts := &appsv1.StatefulSet{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      instance.Spec.ClickHouse.Name,
			Namespace: instance.Namespace,
		}, foundSts)
		if err != nil {
			r.log.Error(err, "get clickhouse statefulset error")
			return err
		}

		if foundSts.Status.ReadyReplicas == instance.Spec.Zookeeper.Replicas {
			r.log.Info("reconcile clickhouse pods finish...")
			return nil
		}

		r.log.Info("reconcile clickhouse pods",
			"ReadyReplicas", foundSts.Status.ReadyReplicas,
			"Replicas", instance.Spec.Zookeeper.Replicas)
		time.Sleep(5 * time.Second)
	}
}

func (r *ReconcileClickHouseCluster) waitZkStatefulSetFinish(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	r.log.Info("reconcile zookeeper pods start...")

	for {
		foundSts := &appsv1.StatefulSet{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      instance.Spec.Zookeeper.Name,
			Namespace: instance.Namespace,
		}, foundSts)
		if err != nil {
			r.log.Error(err, "get zookeeper statefulset error")
			return err
		}

		if foundSts.Status.ReadyReplicas == instance.Spec.Zookeeper.Replicas {
			r.log.Info("reconcile zookeeper pods finish...")
			return nil
		}

		r.log.Info("reconcile zk pods",
			"ReadyReplicas", foundSts.Status.ReadyReplicas,
			"Replicas", instance.Spec.Zookeeper.Replicas)
		time.Sleep(5 * time.Second)
	}
}

func (r *ReconcileClickHouseCluster) reconcileChHeadlessService(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	svc := MakeChHeadlessService(instance, 0, 0)
	if err = controllerutil.SetControllerReference(instance, svc, r.scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new ClickHouse headless service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.client.Create(context.TODO(), svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.log.Info("Updating existing headless service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		SyncService(foundSvc, svc)
		err = r.client.Update(context.TODO(), foundSvc)
		if err != nil {
			return err
		}
	}

	svc = MakeChHeadlessService(instance, 0, 1)
	if err = controllerutil.SetControllerReference(instance, svc, r.scheme); err != nil {
		return err
	}
	foundSvc = &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new ClickHouse headless service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.client.Create(context.TODO(), svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.log.Info("Updating existing headless service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		SyncService(foundSvc, svc)
		err = r.client.Update(context.TODO(), foundSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkHeadlessService(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	svc := MakeHeadlessService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new headless service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.client.Create(context.TODO(), svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.log.Info("Updating existing headless service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		SyncService(foundSvc, svc)
		err = r.client.Update(context.TODO(), foundSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkPodDisruptionBudget(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	pdb := MakePodDisruptionBudget(instance)
	if err = controllerutil.SetControllerReference(instance, pdb, r.scheme); err != nil {
		return err
	}
	foundPdb := &policyv1beta1.PodDisruptionBudget{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      pdb.Name,
		Namespace: pdb.Namespace,
	}, foundPdb)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new pod-disruption-budget",
			"PodDisruptionBudget.Namespace", pdb.Namespace,
			"PodDisruptionBudget.Name", pdb.Name)
		err = r.client.Create(context.TODO(), pdb)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkClusterStatus(instance *clickhousev1beta1.ClickHouseCluster) (err error) {
	if instance.Status.IsClusterInUpgradingState() || instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}
	instance.Status.Init()
	foundPods := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(map[string]string{"app": instance.GetName()})
	listOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	}
	err = r.client.List(context.TODO(), foundPods, listOps)
	if err != nil {
		return err
	}
	var (
		readyMembers   []string
		unreadyMembers []string
	)
	for _, p := range foundPods.Items {
		ready := true
		for _, c := range p.Status.ContainerStatuses {
			if !c.Ready {
				ready = false
			}
		}
		if ready {
			readyMembers = append(readyMembers, p.Name)
		} else {
			unreadyMembers = append(unreadyMembers, p.Name)
		}
	}

	r.log.Info("Updating zookeeper status",
		"StatefulSet.Namespace", instance.Namespace,
		"StatefulSet.Name", instance.Name)
	if instance.Status.ZkReadyReplicas == instance.Spec.Zookeeper.Replicas {
		instance.Status.SetPodsReadyConditionTrue()
	} else {
		instance.Status.SetPodsReadyConditionFalse()
	}

	return r.client.Status().Update(context.TODO(), instance)
}

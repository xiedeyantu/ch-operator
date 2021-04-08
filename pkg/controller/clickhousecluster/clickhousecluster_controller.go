package clickhousecluster

import (
	"context"
	"github.com/go-logr/logr"
	v1beta1 "github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 60 * time.Second

var log = logf.Log.WithName("controller_clickhousecluster")

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
	err = c.Watch(&source.Kind{Type: &v1beta1.ClickHouseCluster{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner ClickHouseCluster
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &v1beta1.ClickHouseCluster{},
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

type reconcileFun func(cluster *v1beta1.ClickHouseCluster) error

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
	instance := &v1beta1.ClickHouseCluster{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
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

	for _, fun := range []reconcileFun{
		//r.reconcileFinalizers,
		r.reconcileChConfigMap,
		r.reconcileChHeadlessService,
		r.reconcileChStatefulSet,

		r.reconcileChClusterStatus,
	} {
		if err = fun(instance); err != nil {
			return reconcile.Result{}, err
		}
	}

	instance.Status.ZookeeperStatus.Resources =
		instance.Spec.Zookeeper.Persistence.PersistentVolumeClaimSpec.Resources
	instance.Status.ZookeeperStatus.Volumes =
		instance.Spec.Zookeeper.Volumes
	instance.Status.ClickHouseStatus.Resources =
		instance.Spec.ClickHouse.Persistence.PersistentVolumeClaimSpec.Resources
	instance.Status.ClickHouseStatus.Volumes =
		instance.Spec.Zookeeper.Volumes

	// Recreate any missing resources every 'ReconcileTime'
	return reconcile.Result{RequeueAfter: ReconcileTime}, nil
}

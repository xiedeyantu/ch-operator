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

package controllers

import (
	"context"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"

	clickhousexiedeyantucomv1beta1 "github.com/xiedeyantu/ch-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 60 * time.Second

// ClickHouseClusterReconciler reconciles a ClickHouseCluster object
type ClickHouseClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=clickhouse.xiedeyantu.com.clickhouse.xiedeyantu.com,resources=clickhouseclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=clickhouse.xiedeyantu.com.clickhouse.xiedeyantu.com,resources=clickhouseclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=clickhouse.xiedeyantu.com.clickhouse.xiedeyantu.com,resources=clickhouseclusters/finalizers,verbs=update

type reconcileFun func(cluster *clickhousexiedeyantucomv1beta1.ClickHouseCluster) error

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ClickHouseCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *ClickHouseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.Info("Reconciling ClickHouseCluster")

	// Fetch the ClickHouseCluster instance
	instance := &clickhousexiedeyantucomv1beta1.ClickHouseCluster{}
	err := r.Client.Get(context.TODO(), req.NamespacedName, instance)
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
		instance.Spec.Zookeeper.Resources
	instance.Status.ZookeeperStatus.Persistence =
		instance.Spec.Zookeeper.Persistence
	instance.Status.ClickHouseStatus.Resources =
		instance.Spec.ClickHouse.Resources
	instance.Status.ClickHouseStatus.Persistence =
		instance.Spec.Zookeeper.Persistence

	_ = r.Client.Status().Update(context.TODO(), instance)
	// Recreate any missing resources every 'ReconcileTime'
	time.Sleep(10 * time.Second)
	return reconcile.Result{RequeueAfter: ReconcileTime}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClickHouseClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clickhousexiedeyantucomv1beta1.ClickHouseCluster{}).
		Complete(r)
}

package clickhousecluster

import (
	"context"
	"github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"
)

func (r *ReconcileClickHouseCluster) reconcileZkConfigMap(instance *v1beta1.ClickHouseCluster) (err error) {
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
		r.log.Info("Creating a new Zookeeper ConfigMap",
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
		r.log.Info("Updating existing ConfigMap",
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

func (r *ReconcileClickHouseCluster) reconcileZkStatefulSet(instance *v1beta1.ClickHouseCluster) (err error) {
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
	} else if err != nil {
		return err
	}

	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkHeadlessService(instance *v1beta1.ClickHouseCluster) (err error) {
	svc := MakeZkHeadlessService(instance)

	if err = controllerutil.SetControllerReference(instance, svc, r.scheme); err != nil {
		return err
	}

	foundSvc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new headless Service",
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
		r.log.Info("Updating existing headless Service",
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

func (r *ReconcileClickHouseCluster) reconcileZkPodDisruptionBudget(instance *v1beta1.ClickHouseCluster) (err error) {
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
	} else if err != nil {
		return err
	}

	return nil
}

func (r *ReconcileClickHouseCluster) reconcileZkClusterStatus(instance *v1beta1.ClickHouseCluster) (err error) {
	if instance.Status.IsClusterInUpgradingState() || instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}

	instance.Status.Init()

	r.log.Info("reconcile zookeeper pods start...")

	instance.Status.ZkReplicas = instance.Spec.Zookeeper.Replicas
	for {
		foundSts := &appsv1.StatefulSet{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      instance.GetZkName(),
			Namespace: instance.Namespace,
		}, foundSts)
		if err != nil {
			r.log.Error(err, "get zookeeper statefulset error")
			return err
		}

		if foundSts.Status.ReadyReplicas == instance.Spec.Zookeeper.Replicas {
			r.log.Info("reconcile zookeeper pods finish...")

			instance.Status.ZkReadyReplicas = foundSts.Status.ReadyReplicas
			instance.Status.ZkReplicas = instance.Spec.Zookeeper.Replicas
			_ = r.client.Status().Update(context.TODO(), instance)

			time.Sleep(1 * time.Second)
			return nil
		}

		r.log.Info("reconcile zk pods",
			"ReadyReplicas", foundSts.Status.ReadyReplicas,
			"Replicas", instance.Spec.Zookeeper.Replicas)

		instance.Status.ZkReadyReplicas = foundSts.Status.ReadyReplicas
		_ = r.client.Status().Update(context.TODO(), instance)

		time.Sleep(5 * time.Second)
	}
}


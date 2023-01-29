package controllers

import (
	"context"
	"github.com/xiedeyantu/ch-operator/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"
)

func (r *ClickHouseClusterReconciler) reconcileZkConfigMap(instance *v1beta1.ClickHouseCluster) (err error) {
	cm := MakeZkConfigMap(instance)

	if err = controllerutil.SetControllerReference(instance, cm, r.Scheme); err != nil {
		return err
	}

	foundCm := &corev1.ConfigMap{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      cm.Name,
		Namespace: cm.Namespace,
	}, foundCm)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating a new Zookeeper ConfigMap",
			"ConfigMap.Namespace", cm.Namespace,
			"ConfigMap.Name", cm.Name)
		err = r.Client.Create(context.TODO(), cm)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		logger.Info("Updating existing ConfigMap",
			"ConfigMap.Namespace", foundCm.Namespace,
			"ConfigMap.Name", foundCm.Name)
		SyncConfigMap(foundCm, cm)
		err = r.Client.Update(context.TODO(), foundCm)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ClickHouseClusterReconciler) reconcileZkStatefulSet(instance *v1beta1.ClickHouseCluster) (err error) {
	if instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}

	sts := MakeStatefulSet(instance)

	if err = controllerutil.SetControllerReference(instance, sts, r.Scheme); err != nil {
		return err
	}

	foundSts := &appsv1.StatefulSet{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	}, foundSts)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating a new Zookeeper StatefulSet",
			"StatefulSet.Namespace", sts.Namespace,
			"StatefulSet.Name", sts.Name)
		err = r.Client.Create(context.TODO(), sts)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}

func (r *ClickHouseClusterReconciler) reconcileZkHeadlessService(instance *v1beta1.ClickHouseCluster) (err error) {
	svc := MakeZkHeadlessService(instance)

	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}

	foundSvc := &corev1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating new headless Service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.Client.Create(context.TODO(), svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		logger.Info("Updating existing headless Service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		SyncService(foundSvc, svc)
		err = r.Client.Update(context.TODO(), foundSvc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ClickHouseClusterReconciler) reconcileZkPodDisruptionBudget(instance *v1beta1.ClickHouseCluster) (err error) {
	pdb := MakePodDisruptionBudget(instance)

	if err = controllerutil.SetControllerReference(instance, pdb, r.Scheme); err != nil {
		return err
	}

	foundPdb := &policyv1beta1.PodDisruptionBudget{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      pdb.Name,
		Namespace: pdb.Namespace,
	}, foundPdb)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating new pod-disruption-budget",
			"PodDisruptionBudget.Namespace", pdb.Namespace,
			"PodDisruptionBudget.Name", pdb.Name)
		err = r.Client.Create(context.TODO(), pdb)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}

func (r *ClickHouseClusterReconciler) reconcileZkClusterStatus(instance *v1beta1.ClickHouseCluster) (err error) {
	if instance.Status.IsClusterInUpgradingState() || instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}

	instance.Status.Init()

	logger.Info("reconcile zookeeper pods start...")

	instance.Status.ZkReplicas = instance.Spec.Zookeeper.Replicas
	for {
		foundSts := &appsv1.StatefulSet{}
		err = r.Client.Get(context.TODO(), types.NamespacedName{
			Name:      instance.GetZkName(),
			Namespace: instance.Namespace,
		}, foundSts)
		if err != nil {
			logger.Error(err, "get zookeeper statefulset error")
			return err
		}

		if foundSts.Status.ReadyReplicas == instance.Spec.Zookeeper.Replicas {
			logger.Info("reconcile zookeeper pods finish...")

			instance.Status.ZkReadyReplicas = foundSts.Status.ReadyReplicas
			instance.Status.ZkReplicas = instance.Spec.Zookeeper.Replicas
			_ = r.Client.Status().Update(context.TODO(), instance)

			time.Sleep(1 * time.Second)
			return nil
		}

		logger.Info("reconcile zk pods",
			"ReadyReplicas", foundSts.Status.ReadyReplicas,
			"Replicas", instance.Spec.Zookeeper.Replicas)

		instance.Status.ZkReadyReplicas = foundSts.Status.ReadyReplicas
		_ = r.Client.Status().Update(context.TODO(), instance)

		time.Sleep(5 * time.Second)
	}
}

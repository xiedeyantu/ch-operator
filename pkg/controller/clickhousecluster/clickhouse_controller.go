package clickhousecluster

import (
	"context"
	"github.com/xiedeyantu/ch-operator/pkg/apis/clickhouse/v1beta1"
	"github.com/xiedeyantu/ch-operator/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"
)

func (r *ReconcileClickHouseCluster) CreateOrUpdateChConfigMap(instance *v1beta1.ClickHouseCluster, configMapType string) (err error) {
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

func (r *ReconcileClickHouseCluster) reconcileChConfigMap(instance *v1beta1.ClickHouseCluster) (err error) {
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

func (r *ReconcileClickHouseCluster) reconcileChStatefulSet(instance *v1beta1.ClickHouseCluster) (err error) {
	clickhouse := instance.Spec.ClickHouse
	expectedShards := clickhouse.Shards
	expectedReplicas := clickhouse.Replicas

	for shard := int32(0); shard < expectedShards; shard++ {
		for replica := int32(0); replica < expectedReplicas; replica++ {
			err = r.CreateOrUpdateChStatefulSet(instance, shard, replica)
			if err != nil {
				return err
			}
		}
	}

	clickhouseStatus := instance.Status.ClickHouseStatus
	currentShards := clickhouseStatus.Shards
	currentReplicas := clickhouseStatus.Replicas

	if currentShards <= expectedShards && currentReplicas <= expectedReplicas {
		r.log.Info("ClickHouse cluster has not change")
		return nil
	}

	r.log.Info("ClickHouse instance expected status",
		"expectedShards", expectedShards, "expectedReplicas", expectedReplicas,
		"currentShards", currentShards, "currentReplicas", currentReplicas)

	if currentShards > expectedShards {
		for shard := currentShards; shard > expectedShards; shard-- {
			for replica := int32(0); replica < currentReplicas; replica++ {
				_ = r.DeleteChStatefulSet(instance, shard - 1, replica)
			}
		}
	}

	if currentReplicas > expectedReplicas {
		for shard := int32(0); shard < expectedShards; shard++ {
			for replica := currentReplicas; replica > expectedReplicas; replica-- {
				_ = r.DeleteChStatefulSet(instance, shard, replica - 1)
			}
		}
	}

	return nil
}

func (r *ReconcileClickHouseCluster) CreateOrUpdateChStatefulSet(instance *v1beta1.ClickHouseCluster, shard, replica int32) (err error) {
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
		r.log.Info("Creating a new ClickHouse instance",
			"StatefulSet.Namespace", sts.Namespace,
			"StatefulSet.Name", sts.Name)
		err = r.client.Create(context.TODO(), sts)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	expectedStorage := instance.Spec.ClickHouse.Persistence.PersistentVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage]
	currentStorage := instance.Status.ClickHouseStatus.Persistence.PersistentVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage]
	if expectedStorage != currentStorage {
		r.log.Info("ClickHouse instance storage change",
			"expectedStorage", expectedStorage,
			"currentStorage", currentStorage,
			"stsName", sts.Name)
		err = r.ScaleStorage(sts, "data", expectedStorage)
		if err != nil {
			return err
		}
	}

	r.log.Info("ClickHouse instance exist",
		"StatefulSet.Namespace", sts.Namespace,
		"StatefulSet.Name", sts.Name)
	return nil
}

func (r *ReconcileClickHouseCluster) DeleteChStatefulSet(instance *v1beta1.ClickHouseCluster, shard, replica int32) (err error) {
	stsName := instance.GetChStatefulSetName(shard, replica)
	r.log.Info("delete sts","stsName",stsName)

	foundSts := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      stsName,
		Namespace: instance.Namespace,
	}, foundSts)

	if err != nil {
		r.log.Error(err, "get clickhouse statefulset error")
		return err
	}

	err = r.client.Delete(context.TODO(), foundSts)
	if err != nil {
		r.log.Error(err, "Error deleteing clickhouse statefulset.", "Name", stsName)
	}
	return nil
}

func (r *ReconcileClickHouseCluster) reconcileChHeadlessService(instance *v1beta1.ClickHouseCluster) (err error) {
	clickhouse := instance.Spec.ClickHouse
	expectedShards := clickhouse.Shards
	expectedReplicas := clickhouse.Replicas

	for shard := int32(0); shard < expectedShards; shard++ {
		for replica := int32(0); replica < expectedReplicas; replica++ {
			err = r.CreateOrUpdateChHeadService(instance, shard, replica)
			if err != nil {
				return err
			}
		}
	}

	clickhouseStatus := instance.Status.ClickHouseStatus
	currentShards := clickhouseStatus.Shards
	currentReplicas := clickhouseStatus.Replicas

	if currentShards <= expectedShards && currentReplicas <= expectedReplicas {
		r.log.Info("ClickHouse cluster service has not change")
		return nil
	}

	r.log.Info("ClickHouse instance service expected status",
		"expectedShards", expectedShards, "expectedReplicas", expectedReplicas,
		"currentShards", currentShards, "currentReplicas", currentReplicas)

	if currentShards > expectedShards {
		for shard := currentShards; shard > expectedShards; shard-- {
			for replica := int32(0); replica < currentReplicas; replica++ {
				_ = r.DeleteChHeadService(instance, shard - 1, replica)
			}
		}
	}

	if currentReplicas > expectedReplicas {
		for shard := int32(0); shard < expectedReplicas; shard++ {
			for replica := currentReplicas; replica > expectedReplicas; replica-- {
				_ = r.DeleteChHeadService(instance, shard, replica - 1)
			}
		}
	}

	return nil
}

func (r *ReconcileClickHouseCluster) CreateOrUpdateChHeadService(instance *v1beta1.ClickHouseCluster, shard, replica int32) (err error) {
	svc := MakeChHeadlessService(instance, shard, replica)

	if err = controllerutil.SetControllerReference(instance, svc, r.scheme); err != nil {
		return err
	}

	foundSvc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.log.Info("Creating new ClickHouse headless Service",
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

func (r *ReconcileClickHouseCluster) DeleteChHeadService(instance *v1beta1.ClickHouseCluster, shard, replica int32) (err error) {
	svcName := instance.GetChStatefulSetName(shard, replica)
	r.log.Info("delete svc","svsName",svcName)

	foundSvc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{
		Name:      svcName,
		Namespace: instance.Namespace,
	}, foundSvc)

	if err != nil {
		r.log.Error(err, "get clickhouse headless service error")
		return err
	}

	err = r.client.Delete(context.TODO(), foundSvc)
	if err != nil {
		r.log.Error(err, "Error deleteing clickhouse headless service.", "Name", svcName)
	}
	return nil
}

func (r *ReconcileClickHouseCluster) waitChStatefulSetFinish(instance *v1beta1.ClickHouseCluster) (err error) {
	r.log.Info("reconcile clickhouse pods start...")

	for shard := int32(0); shard < instance.Spec.ClickHouse.Shards; shard++ {
		for replica := int32(0); replica < instance.Spec.ClickHouse.Replicas; replica++ {
			err = r.pollChStatefulSet(instance, shard, replica)
			if err != nil {
				return err
			}
		}
	}

	r.log.Info("reconcile clickhouse pods finish...")

	return nil
}

func (r *ReconcileClickHouseCluster) pollChStatefulSet(instance *v1beta1.ClickHouseCluster, shard, replica int32) (err error) {
	r.log.Info("reconcile clickhouse pod start...", "shard", shard, "replica", replica)

	for {
		foundSts := &appsv1.StatefulSet{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      instance.GetChStatefulSetName(shard, replica),
			Namespace: instance.Namespace,
		}, foundSts)
		if err != nil {
			r.log.Error(err, "get clickhouse statefulset error")
			return err
		}

		if foundSts.Status.ReadyReplicas == *foundSts.Spec.Replicas {
			r.log.Info("reconcile clickhouse pod finish...", "shard", shard, "replica", replica)
			return nil
		}

		r.log.Info("reconcile clickhouse pod",
			"shard", shard,
			"replica", replica)

		time.Sleep(5 * time.Second)
	}
}

func (r *ReconcileClickHouseCluster) reconcileChClusterStatus(instance *v1beta1.ClickHouseCluster) (err error) {
	if instance.Status.IsClusterInUpgradingState() || instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}

	instance.Status.Init()
	instance.Status.ClickHouseStatus.Shards = instance.Spec.ClickHouse.Shards
	instance.Status.ClickHouseStatus.Replicas = instance.Spec.ClickHouse.Replicas
	_ = r.client.Status().Update(context.TODO(), instance)
	r.log.Info("reconcile clickhouse pods start...")

	instance.Status.ChNodes = instance.Spec.ClickHouse.Shards * instance.Spec.ClickHouse.Replicas
	for shard := int32(0); shard < instance.Spec.ClickHouse.Shards; shard++ {
		for replica := int32(0); replica < instance.Spec.ClickHouse.Replicas; replica++ {
			err = r.pollChStatefulSet(instance, shard, replica)
			if err != nil {
				return err
			}
			instance.Status.ChReadyNodes = (shard + 1) * (replica + 1)
			_ = r.client.Status().Update(context.TODO(), instance)
		}
	}

	r.log.Info("reconcile clickhouse pods finish...")

	instance.Status.SetPodsReadyConditionTrue()
	_ = r.client.Status().Update(context.TODO(), instance)

	return nil
}



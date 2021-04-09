package clickhousecluster

import (
	"context"
	"errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"time"
)

func (r *ReconcileClickHouseCluster) ScaleStorage(workLoad interface{}, volumeName string, storage resource.Quantity) error {
	switch workLoad.(type) {
	case *appsv1.StatefulSet:
		sts := workLoad.(*appsv1.StatefulSet)
		return r.StatefulSetScaleStorage(sts, volumeName, storage)
	case *appsv1.Deployment:
		workLoad = workLoad.(*appsv1.Deployment)
		return nil
	default:
		message := "Type not found"
		r.log.Info(message)
		return errors.New(message)
	}
}

func (r *ReconcileClickHouseCluster) StatefulSetScaleStorage(sts *appsv1.StatefulSet, volumeName string, storage resource.Quantity) error {
	deletePolicy := metav1.DeletePropagationOrphan
	err := r.client.Delete(context.TODO(), sts, &client.DeleteOptions{
		PropagationPolicy:  &deletePolicy,
	})
	if err != nil {
		r.log.Error(err, "Delete StatefulSet error",
			"StatefulSet.Name", sts.Name)
		return err
	}

	matchLabels := sts.Spec.Selector.MatchLabels
	podList := &corev1.PodList{}
	err = r.client.List(context.TODO(), podList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(matchLabels),
	})
	if err != nil {
		r.log.Error(err, "List Pods error",
			"Labels", matchLabels)
		return err
	}

	storageRequest := map[corev1.ResourceName]resource.Quantity{}
	storageRequest[corev1.ResourceStorage] = storage

	var pvcList []*corev1.PersistentVolumeClaim
	for _, pod := range podList.Items {
		var pvcName string
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == volumeName {
				pvcName = volume.PersistentVolumeClaim.ClaimName
			}
		}

		pvc := &corev1.PersistentVolumeClaim{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      pvcName,
			Namespace: pod.Namespace,
		}, pvc)
		if err != nil {
			r.log.Error(err, "Get Pod PersistentVolumeClaim error",
				"Pod.Name", pod.Name)
			return err
		}

		pvc.Spec.Resources.Requests = storageRequest
		err = r.client.Update(context.TODO(), pvc)
		if err != nil {
			r.log.Error(err, "Update Pod PersistentVolumeClaim error",
				"PodName", pod.Name)
			return err
		}

		pvcList = append(pvcList, pvc)
	}

	var wg sync.WaitGroup
	wg.Add(len(pvcList))
	for _, pvc := range pvcList {
		go func() {
			err = r.pollPvc(pvc, storage)
			if err != nil {
				r.log.Error(err, "Polling Pod PersistentVolumeClaim error",
					"pvcName", pvc.Name)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	sts.ResourceVersion = ""
	sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests = storageRequest
	time.Sleep(5 * time.Second)
	err = r.client.Create(context.TODO(), sts)
	if err != nil {
		r.log.Error(err, "ReCreate StatefulSet error",
			"StatefulSet.Name", sts.Name)
		return err
	}

	r.log.Info("Scale StatefulSet storage successfully",
		"StatefulSet.Namespace", sts.Namespace,
		"StatefulSet.Name", sts.Name)
	return nil
}

func (r *ReconcileClickHouseCluster) pollPvc(pvc *corev1.PersistentVolumeClaim, storage resource.Quantity) (err error) {
	r.log.Info("Polling pvc start...", "pvcName", pvc.Name)

	for {
		foundPvc := &corev1.PersistentVolumeClaim{}
		err = r.client.Get(context.TODO(), types.NamespacedName{
			Name:      pvc.Name,
			Namespace: pvc.Namespace,
		}, foundPvc)
		if err != nil {
			r.log.Error(err, "Get pvc error")
			return err
		}

		if foundPvc.Status.Capacity[corev1.ResourceStorage] == storage {
			r.log.Info("Polling pvc finish...",
				"pvcName", pvc.Name, "storage", storage)
			return nil
		}

		time.Sleep(5 * time.Second)
	}
}
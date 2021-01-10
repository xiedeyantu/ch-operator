/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package v1beta1

import (
	"time"

	v1 "k8s.io/api/core/v1"
)

type ClusterConditionType string

const (
	ClusterConditionPodsReady ClusterConditionType = "PodsReady"
	ClusterConditionUpgrading                      = "Upgrading"
	ClusterConditionError                          = "Error"

	// Reasons for cluster upgrading condition
)

// ClusterCondition shows the current condition of a Zookeeper cluster.
// Comply with k8s API conventions
type ClusterCondition struct {
	Type ClusterConditionType `json:"type,omitempty"`
	// Status of the condition, one of True, False, Unknown.
	Status             v1.ConditionStatus `json:"status,omitempty"`
	Reason             string             `json:"reason,omitempty"`
	Message            string             `json:"message,omitempty"`
	LastUpdateTime     string             `json:"lastUpdateTime,omitempty"`
	LastTransitionTime string             `json:"lastTransitionTime,omitempty"`
}

func (cs *ClickHouseClusterStatus) Init() {
	// Initialise conditions
	conditionTypes := []ClusterConditionType{
		ClusterConditionPodsReady,
		ClusterConditionUpgrading,
		ClusterConditionError,
	}
	for _, conditionType := range conditionTypes {
		if _, condition := cs.GetClusterCondition(conditionType); condition == nil {
			c := newClusterCondition(conditionType, v1.ConditionFalse, "", "")
			cs.setClusterCondition(*c)
		}
	}
}

func newClusterCondition(condType ClusterConditionType, status v1.ConditionStatus, reason, message string) *ClusterCondition {
	return &ClusterCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastUpdateTime:     "",
		LastTransitionTime: "",
	}
}

func (cs *ClickHouseClusterStatus) SetPodsReadyConditionTrue() {
	c := newClusterCondition(ClusterConditionPodsReady, v1.ConditionTrue, "", "")
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) SetPodsReadyConditionFalse() {
	c := newClusterCondition(ClusterConditionPodsReady, v1.ConditionFalse, "", "")
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) SetUpgradingConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionUpgrading, v1.ConditionTrue, reason, message)
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) SetUpgradingConditionFalse() {
	c := newClusterCondition(ClusterConditionUpgrading, v1.ConditionFalse, "", "")
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) SetErrorConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionError, v1.ConditionTrue, reason, message)
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) SetErrorConditionFalse() {
	c := newClusterCondition(ClusterConditionError, v1.ConditionFalse, "", "")
	cs.setClusterCondition(*c)
}

func (cs *ClickHouseClusterStatus) GetClusterCondition(t ClusterConditionType) (int, *ClusterCondition) {
	for i, c := range cs.Conditions {
		if t == c.Type {
			return i, &c
		}
	}
	return -1, nil
}

func (cs *ClickHouseClusterStatus) setClusterCondition(newCondition ClusterCondition) {
	now := time.Now().Format(time.RFC3339)
	position, existingCondition := cs.GetClusterCondition(newCondition.Type)

	if existingCondition == nil {
		cs.Conditions = append(cs.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = now
		existingCondition.LastUpdateTime = now
	}

	if existingCondition.Reason != newCondition.Reason || existingCondition.Message != newCondition.Message {
		existingCondition.Reason = newCondition.Reason
		existingCondition.Message = newCondition.Message
		existingCondition.LastUpdateTime = now
	}

	cs.Conditions[position] = *existingCondition
}

func (cs *ClickHouseClusterStatus) IsClusterInUpgradeFailedState() bool {
	_, errorCondition := cs.GetClusterCondition(ClusterConditionError)
	if errorCondition == nil {
		return false
	}
	if errorCondition.Status == v1.ConditionTrue && errorCondition.Reason == "UpgradeFailed" {
		return true
	}
	return false
}

func (cs *ClickHouseClusterStatus) IsClusterInUpgradingState() bool {
	_, upgradeCondition := cs.GetClusterCondition(ClusterConditionUpgrading)
	if upgradeCondition == nil {
		return false
	}
	if upgradeCondition.Status == v1.ConditionTrue {
		return true
	}
	return false
}

func (cs *ClickHouseClusterStatus) IsClusterInReadyState() bool {
	_, readyCondition := cs.GetClusterCondition(ClusterConditionPodsReady)
	if readyCondition != nil && readyCondition.Status == v1.ConditionTrue {
		return true
	}
	return false
}

func (cs *ClickHouseClusterStatus) UpdateProgress(reason, updatedReplicas string) {
	if cs.IsClusterInUpgradingState() {
		// Set the upgrade condition reason to be UpgradingZookeeperReason, message to be the upgradedReplicas
		cs.SetUpgradingConditionTrue(reason, updatedReplicas)
	}
}

func (cs *ClickHouseClusterStatus) GetLastCondition() (lastCondition *ClusterCondition) {
	if cs.IsClusterInUpgradingState() {
		_, lastCondition := cs.GetClusterCondition(ClusterConditionUpgrading)
		return lastCondition
	}
	// nothing to do if we are not upgrading
	return nil
}

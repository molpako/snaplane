/*
Copyright 2026.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SerialDispatchMode string

const (
	SerialDispatchModeOnly SerialDispatchMode = "SerialOnly"
)

type BackupSource struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistentVolumeClaimName is immutable"
	PersistentVolumeClaimName string `json:"persistentVolumeClaimName"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeSnapshotClassName is immutable"
	VolumeSnapshotClassName string `json:"volumeSnapshotClassName"`
}

type BackupSchedule struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Cron string `json:"cron"`

	// +optional
	TimeZone *string `json:"timeZone,omitempty"`

	// +optional
	// +kubebuilder:default:=false
	Suspend *bool `json:"suspend,omitempty"`
}

type BackupDispatch struct {
	// +optional
	// +kubebuilder:validation:Enum=SerialOnly
	// +kubebuilder:default:=SerialOnly
	Mode SerialDispatchMode `json:"mode,omitempty"`
}

type BackupRetention struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	KeepDays int32 `json:"keepDays"`
}

type TransferPolicy struct {
	// +optional
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=60
	// +kubebuilder:default:=6
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`
}

type BackupPolicyManualSpec struct {
	// +optional
	RequestID string `json:"requestID,omitempty"`
}

type BackupPolicyManualStatus struct {
	LastHandledRequestID string       `json:"lastHandledRequestID,omitempty"`
	LastHandledTime      *metav1.Time `json:"lastHandledTime,omitempty"`
}

// BackupPolicySpec defines the desired state of BackupPolicy.
type BackupPolicySpec struct {
	// +kubebuilder:validation:Required
	Source BackupSource `json:"source"`

	// +kubebuilder:validation:Required
	Schedule BackupSchedule `json:"schedule"`

	// +optional
	Dispatch BackupDispatch `json:"dispatch,omitempty"`

	// +kubebuilder:validation:Required
	Retention BackupRetention `json:"retention"`

	// +optional
	Transfer TransferPolicy `json:"transfer,omitempty"`

	// +optional
	Manual BackupPolicyManualSpec `json:"manual,omitempty"`
}

// BackupPolicyStatus defines the observed state of BackupPolicy.
type BackupPolicyStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	LastScheduledTime      *metav1.Time             `json:"lastScheduledTime,omitempty"`
	LastDispatchedSnapshot string                   `json:"lastDispatchedSnapshot,omitempty"`
	ActiveBackupName       string                   `json:"activeBackupName,omitempty"`
	PendingSnapshotCount   int32                    `json:"pendingSnapshotCount,omitempty"`
	Manual                 BackupPolicyManualStatus `json:"manual,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=".spec.schedule.cron",name=Schedule,type=string
// +kubebuilder:printcolumn:JSONPath=".status.activeBackupName",name=ActiveBackup,type=string
// +kubebuilder:printcolumn:JSONPath=".status.pendingSnapshotCount",name=PendingVS,type=integer
// +kubebuilder:printcolumn:JSONPath=".metadata.creationTimestamp",name=Age,type=date

// BackupPolicy is the Schema for the backuppolicies API.
type BackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec   BackupPolicySpec   `json:"spec"`
	Status BackupPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupPolicyList contains a list of BackupPolicy.
type BackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackupPolicy{}, &BackupPolicyList{})
}

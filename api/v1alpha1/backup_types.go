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

type LocalNameReference struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type VolumeSnapshotReference struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// +optional
	Namespace *string `json:"namespace,omitempty"`
}

type BackupDestination struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="destination is immutable"
	NodeName string `json:"nodeName"`
}

// BackupSpec defines the desired state of Backup.
type BackupSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="policyRef is immutable"
	PolicyRef LocalNameReference `json:"policyRef"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeSnapshotRef is immutable"
	VolumeSnapshotRef VolumeSnapshotReference `json:"volumeSnapshotRef"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="destination is immutable"
	Destination BackupDestination `json:"destination"`

	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="transfer is immutable"
	Transfer TransferPolicy `json:"transfer,omitempty"`
}

type BackupStats struct {
	BytesTransferred int64 `json:"bytesTransferred,omitempty"`
}

type BackupProgressStatus struct {
	TotalBytes int64 `json:"totalBytes,omitempty"`
	BytesDone  int64 `json:"bytesDone,omitempty"`

	IncrementalBytes int64 `json:"incrementalBytes,omitempty"`
}

type BackupRetryStatus struct {
	Attempt int32 `json:"attempt,omitempty"`

	LastFailureReason string       `json:"lastFailureReason,omitempty"`
	LastFailureTime   *metav1.Time `json:"lastFailureTime,omitempty"`
}

type BackupRestoreSourceStatus struct {
	NodeName        string   `json:"nodeName,omitempty"`
	RepositoryPath  string   `json:"repositoryPath,omitempty"`
	ManifestID      string   `json:"manifestID,omitempty"`
	ManifestChain   []string `json:"manifestChain,omitempty"`
	RepoUUID        string   `json:"repoUUID,omitempty"`
	Format          string   `json:"format,omitempty"`
	VolumeSizeBytes int64    `json:"volumeSizeBytes,omitempty"`
	ChunkSizeBytes  int64    `json:"chunkSizeBytes,omitempty"`
}

// BackupStatus defines the observed state of Backup.
type BackupStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	StartTime      *metav1.Time              `json:"startTime,omitempty"`
	CompletionTime *metav1.Time              `json:"completionTime,omitempty"`
	LastError      string                    `json:"lastError,omitempty"`
	Stats          BackupStats               `json:"stats,omitempty"`
	Progress       BackupProgressStatus      `json:"progress,omitempty"`
	Retry          BackupRetryStatus         `json:"retry,omitempty"`
	RestoreSource  BackupRestoreSourceStatus `json:"restoreSource,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=".spec.volumeSnapshotRef.name",name=Snapshot,type=string
// +kubebuilder:printcolumn:JSONPath=".status.progress.bytesDone",name=BytesDone,type=integer,format=int64
// +kubebuilder:printcolumn:JSONPath=".status.progress.totalBytes",name=TotalBytes,type=integer,format=int64
// +kubebuilder:printcolumn:JSONPath=".status.retry.attempt",name=Retry,type=integer
// +kubebuilder:printcolumn:JSONPath=".metadata.creationTimestamp",name=Age,type=date

// Backup is the Schema for the backups API.
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec   BackupSpec   `json:"spec"`
	Status BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupList contains a list of Backup.
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Backup{}, &BackupList{})
}

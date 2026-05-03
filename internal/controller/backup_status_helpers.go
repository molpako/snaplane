package controller

import (
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
)

func backupConditionStatus(backup *snaplanev1alpha1.Backup, conditionType string) metav1.ConditionStatus {
	if backup == nil {
		return metav1.ConditionUnknown
	}
	cond := meta.FindStatusCondition(backup.Status.Conditions, conditionType)
	if cond == nil {
		return metav1.ConditionUnknown
	}
	return cond.Status
}

func backupIsSucceeded(backup *snaplanev1alpha1.Backup) bool {
	return backupConditionStatus(backup, snaplanev1alpha1.BackupConditionSucceeded) == metav1.ConditionTrue
}

func backupIsFailed(backup *snaplanev1alpha1.Backup) bool {
	return backupConditionStatus(backup, snaplanev1alpha1.BackupConditionSucceeded) == metav1.ConditionFalse
}

func backupIsTerminal(backup *snaplanev1alpha1.Backup) bool {
	if backup == nil || backup.Status.CompletionTime == nil {
		return false
	}
	status := backupConditionStatus(backup, snaplanev1alpha1.BackupConditionSucceeded)
	return status == metav1.ConditionTrue || status == metav1.ConditionFalse
}

func backupIsActive(backup *snaplanev1alpha1.Backup) bool {
	return !backupIsTerminal(backup)
}

func backupRestoreSourceReady(backup *snaplanev1alpha1.Backup) bool {
	if backup == nil {
		return false
	}
	rs := backup.Status.RestoreSource
	return rs.NodeName != "" &&
		rs.RepositoryPath != "" &&
		rs.ManifestID != "" &&
		rs.RepoUUID != "" &&
		rs.Format != ""
}

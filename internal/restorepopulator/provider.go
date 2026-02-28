package restorepopulator

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	populatorMachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const (
	backupKind = "Backup"

	defaultBackupRoot         = "/var/backup"
	defaultWorkerTargetDevice = "/dev/restore-target"

	workerPodPrefix     = "restore"
	workerPodLabelKey   = "snaplane.molpako.github.io/restore-worker"
	workerContainerName = "restore-worker"
)

// Provider wires lib-volume-populator callbacks to restore-worker pods.
type Provider struct {
	WorkerImage           string
	WorkerImagePullPolicy corev1.PullPolicy
	BackupRoot            string
	WorkerTargetDevice    string
}

func (p *Provider) PopulateFn(ctx context.Context, params populatorMachinery.PopulatorParams) error {
	backup, err := p.validateAcceptedBackup(params)
	if err != nil {
		return err
	}

	podNamespace := params.PvcPrime.Namespace
	podName := workerPodName(params.Pvc.UID)
	pods := params.KubeClient.CoreV1().Pods(podNamespace)

	existing, err := pods.Get(ctx, podName, metav1.GetOptions{})
	if err == nil {
		if existing.Status.Phase != corev1.PodFailed {
			return nil
		}
		if deleteErr := pods.Delete(ctx, podName, metav1.DeleteOptions{}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			return fmt.Errorf("delete failed restore worker pod: %w", deleteErr)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get restore worker pod: %w", err)
	}

	pod, err := p.buildWorkerPod(params, backup)
	if err != nil {
		return err
	}
	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create restore worker pod: %w", err)
	}
	return nil
}

func (p *Provider) PopulateCompleteFn(ctx context.Context, params populatorMachinery.PopulatorParams) (bool, error) {
	if _, err := p.validateAcceptedBackup(params); err != nil {
		return false, err
	}

	podNamespace := params.PvcPrime.Namespace
	podName := workerPodName(params.Pvc.UID)
	pod, err := params.KubeClient.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get restore worker pod: %w", err)
	}

	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return true, nil
	case corev1.PodFailed:
		return false, fmt.Errorf("restore worker pod failed: %s", podFailureMessage(pod))
	default:
		return false, nil
	}
}

func (p *Provider) PopulateCleanupFn(ctx context.Context, params populatorMachinery.PopulatorParams) error {
	if err := validatePopulatorParams(params); err != nil {
		return err
	}
	podNamespace := params.PvcPrime.Namespace
	podName := workerPodName(params.Pvc.UID)
	err := params.KubeClient.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete restore worker pod: %w", err)
	}
	return nil
}

func (p *Provider) validateAcceptedBackup(params populatorMachinery.PopulatorParams) (*snaplanev1alpha1.Backup, error) {
	if err := validatePopulatorParams(params); err != nil {
		return nil, err
	}
	if params.Pvc.Spec.VolumeMode == nil || *params.Pvc.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		return nil, fmt.Errorf("restore populator only supports PVC volumeMode=Block")
	}
	backup, err := decodeBackup(params)
	if err != nil {
		return nil, err
	}
	if err := validateBackupCompleted(backup); err != nil {
		return nil, err
	}
	if err := validateRestoreSource(backup.Status.RestoreSource, p.effectiveBackupRoot()); err != nil {
		return nil, err
	}
	return backup, nil
}

func validatePopulatorParams(params populatorMachinery.PopulatorParams) error {
	if params.KubeClient == nil {
		return fmt.Errorf("kube client is required")
	}
	if params.Pvc == nil {
		return fmt.Errorf("target pvc is required")
	}
	if params.PvcPrime == nil {
		return fmt.Errorf("pvcPrime is required")
	}
	if params.Unstructured == nil {
		return fmt.Errorf("backup datasource is required")
	}
	return nil
}

func decodeBackup(params populatorMachinery.PopulatorParams) (*snaplanev1alpha1.Backup, error) {
	backup := &snaplanev1alpha1.Backup{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(params.Unstructured.UnstructuredContent(), backup); err != nil {
		return nil, fmt.Errorf("decode backup datasource: %w", err)
	}
	return backup, nil
}

func validateBackupCompleted(backup *snaplanev1alpha1.Backup) error {
	if backup == nil {
		return fmt.Errorf("backup is nil")
	}
	succeeded := meta.FindStatusCondition(backup.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
	if succeeded == nil {
		return fmt.Errorf("backup %s/%s is not succeeded", backup.Namespace, backup.Name)
	}
	if succeeded.Status == metav1.ConditionFalse {
		return fmt.Errorf("backup %s/%s is failed", backup.Namespace, backup.Name)
	}
	if succeeded.Status != metav1.ConditionTrue {
		return fmt.Errorf("backup %s/%s is not succeeded", backup.Namespace, backup.Name)
	}
	return nil
}

func validateRestoreSource(rs snaplanev1alpha1.BackupRestoreSourceStatus, backupRoot string) error {
	if rs.NodeName == "" {
		return fmt.Errorf("backup status.restoreSource.nodeName is required")
	}
	if rs.RepositoryPath == "" {
		return fmt.Errorf("backup status.restoreSource.repositoryPath is required")
	}
	if rs.ManifestID == "" {
		return fmt.Errorf("backup status.restoreSource.manifestID is required")
	}
	if rs.RepoUUID == "" {
		return fmt.Errorf("backup status.restoreSource.repoUUID is required")
	}
	switch rs.Format {
	case "", snaplanev1alpha1.RestoreSourceFormatMockImageV1:
		// accepted in phase1 for backward compatibility
	case snaplanev1alpha1.RestoreSourceFormatCASV1:
		// accepted in phase2 for CAS restore
	default:
		return fmt.Errorf("backup restoreSource format %q is unknown", rs.Format)
	}

	repoPath := filepath.Clean(rs.RepositoryPath)
	if !filepath.IsAbs(repoPath) {
		return fmt.Errorf("backup repositoryPath must be absolute: %q", rs.RepositoryPath)
	}
	root := filepath.Clean(backupRoot)
	rel, err := filepath.Rel(root, repoPath)
	if err != nil {
		return fmt.Errorf("resolve restore source path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("backup repositoryPath %q is outside backup root %q", repoPath, root)
	}

	manifestPath := filepath.Clean(filepath.Join(repoPath, rs.ManifestID))
	manifestRel, err := filepath.Rel(repoPath, manifestPath)
	if err != nil {
		return fmt.Errorf("resolve restore manifest path: %w", err)
	}
	if manifestRel == ".." || strings.HasPrefix(manifestRel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("manifestID %q escapes repositoryPath", rs.ManifestID)
	}
	return nil
}

func (p *Provider) buildWorkerPod(params populatorMachinery.PopulatorParams, backup *snaplanev1alpha1.Backup) (*corev1.Pod, error) {
	if p.WorkerImage == "" {
		return nil, fmt.Errorf("worker image is required")
	}

	hostPathType := corev1.HostPathDirectoryOrCreate
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerPodName(params.Pvc.UID),
			Namespace: params.PvcPrime.Namespace,
			Labels: map[string]string{
				workerPodLabelKey: string(params.Pvc.UID),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      backup.Status.RestoreSource.NodeName,
			Containers: []corev1.Container{
				{
					Name:            workerContainerName,
					Image:           p.WorkerImage,
					ImagePullPolicy: p.effectiveImagePullPolicy(),
					Command:         []string{"/restore-populator"},
					Args: []string{
						"--mode=worker",
						"--source-dir=" + backup.Status.RestoreSource.RepositoryPath,
						"--manifest-id=" + backup.Status.RestoreSource.ManifestID,
						"--target-device=" + p.effectiveTargetDevice(),
						"--restore-format=" + backup.Status.RestoreSource.Format,
						"--volume-size-bytes=" + strconv.FormatInt(backup.Status.RestoreSource.VolumeSizeBytes, 10),
						"--chunk-size-bytes=" + strconv.FormatInt(backup.Status.RestoreSource.ChunkSizeBytes, 10),
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsNonRoot: ptr(false),
						RunAsUser:    ptr[int64](0),
					},
					VolumeDevices: []corev1.VolumeDevice{
						{
							Name:       "target-pvc",
							DevicePath: p.effectiveTargetDevice(),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "backup-root",
							MountPath: p.effectiveBackupRoot(),
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "target-pvc",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: params.PvcPrime.Name},
					},
				},
				{
					Name: "backup-root",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: p.effectiveBackupRoot(),
							Type: &hostPathType,
						},
					},
				},
			},
		},
	}
	return pod, nil
}

func podFailureMessage(pod *corev1.Pod) string {
	if pod == nil {
		return "unknown"
	}
	if pod.Status.Message != "" {
		return pod.Status.Message
	}
	if pod.Status.Reason != "" {
		return pod.Status.Reason
	}
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil {
			continue
		}
		if term.Message != "" {
			return term.Message
		}
		if term.Reason != "" {
			return term.Reason
		}
	}
	return "restore worker failed"
}

func workerPodName(pvcUID types.UID) string {
	return fmt.Sprintf("%s-%s", workerPodPrefix, pvcUID)
}

func (p *Provider) effectiveImagePullPolicy() corev1.PullPolicy {
	if p.WorkerImagePullPolicy != "" {
		return p.WorkerImagePullPolicy
	}
	return corev1.PullIfNotPresent
}

func (p *Provider) effectiveBackupRoot() string {
	if p.BackupRoot != "" {
		return p.BackupRoot
	}
	return defaultBackupRoot
}

func (p *Provider) effectiveTargetDevice() string {
	if p.WorkerTargetDevice != "" {
		return p.WorkerTargetDevice
	}
	return defaultWorkerTargetDevice
}

func ptr[T any](value T) *T {
	return &value
}

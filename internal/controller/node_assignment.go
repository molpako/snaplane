package controller

import (
	"cmp"
	"context"
	"os"
	"slices"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

const (
	backupNodeLeaseStaleThreshold = 90 * time.Second
	minimumBackupAvailableBytes   = int64(10 * 1024 * 1024 * 1024)
	unavailableNodeName           = "unavailable"
)

type backupNodeCandidate struct {
	name           string
	writerEndpoint string
	usedBytes      int64
	availableBytes int64
}

func (r *BackupPolicyReconciler) resolveBackupDestinationNode(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	now time.Time,
) (string, error) {
	pvcNN := types.NamespacedName{Namespace: policy.Namespace, Name: policy.Spec.Source.PersistentVolumeClaimName}
	leaseNamespace := r.getLeaseNamespace(policy)

	for attempt := 0; attempt < 5; attempt++ {
		var pvc corev1.PersistentVolumeClaim
		if err := r.Get(ctx, pvcNN, &pvc); err != nil {
			if apierrors.IsNotFound(err) {
				return unavailableNodeName, nil
			}
			return "", err
		}

		assigned := ""
		if pvc.Annotations != nil {
			assigned = pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey]
		}

		candidates, err := r.listBackupNodeCandidates(ctx, leaseNamespace, now)
		if err != nil {
			return "", err
		}

		if assigned != "" {
			if candidateExists(candidates, assigned) {
				return assigned, nil
			}
			return unavailableNodeName, nil
		}
		if len(candidates) == 0 {
			return unavailableNodeName, nil
		}

		chosen := chooseBackupNode(candidates)
		base := pvc.DeepCopy()
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] = chosen.name
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey] = now.UTC().Format(time.RFC3339)

		if err := r.Patch(ctx, &pvc, client.MergeFrom(base)); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return "", err
		}
		return chosen.name, nil
	}

	return "", apierrors.NewConflict(corev1.Resource("persistentvolumeclaims"), policy.Spec.Source.PersistentVolumeClaimName, nil)
}

func candidateExists(candidates []backupNodeCandidate, nodeName string) bool {
	for i := range candidates {
		if candidates[i].name == nodeName {
			return true
		}
	}
	return false
}

func chooseBackupNode(candidates []backupNodeCandidate) backupNodeCandidate {
	sorted := slices.Clone(candidates)
	slices.SortFunc(sorted, func(a, b backupNodeCandidate) int {
		if a.usedBytes != b.usedBytes {
			return cmp.Compare(a.usedBytes, b.usedBytes)
		}
		return cmp.Compare(a.name, b.name)
	})
	return sorted[0]
}

func (r *BackupPolicyReconciler) listBackupNodeCandidates(
	ctx context.Context,
	leaseNamespace string,
	now time.Time,
) ([]backupNodeCandidate, error) {
	nodeSelector := labels.SelectorFromSet(map[string]string{snaplanev1alpha1.BackupTargetNodeLabelKey: "true"})
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes, &client.ListOptions{LabelSelector: nodeSelector}); err != nil {
		return nil, err
	}

	var leases coordinationv1.LeaseList
	if err := r.List(ctx, &leases, &client.ListOptions{Namespace: leaseNamespace}); err != nil {
		return nil, err
	}
	leaseByName := map[string]*coordinationv1.Lease{}
	for i := range leases.Items {
		item := leases.Items[i]
		leaseByName[item.Name] = &item
	}

	candidates := make([]backupNodeCandidate, 0, len(nodes.Items))
	for i := range nodes.Items {
		node := nodes.Items[i]
		if !nodeIsReady(&node) {
			continue
		}

		lease := leaseByName[writer.LeaseNameForNode(node.Name)]
		if lease == nil || lease.Spec.RenewTime == nil {
			continue
		}
		if now.UTC().Sub(lease.Spec.RenewTime.Time.UTC()) > backupNodeLeaseStaleThreshold {
			continue
		}

		annotations := lease.GetAnnotations()
		endpoint := annotations[snaplanev1alpha1.WriterEndpointAnnotationKey]
		if endpoint == "" {
			continue
		}
		used, usedErr := strconv.ParseInt(annotations[snaplanev1alpha1.LeaseUsedBytesAnnotationKey], 10, 64)
		available, availErr := strconv.ParseInt(annotations[snaplanev1alpha1.LeaseAvailableBytesAnnotationKey], 10, 64)
		if usedErr != nil || availErr != nil {
			continue
		}
		if available < minimumBackupAvailableBytes {
			continue
		}

		candidates = append(candidates, backupNodeCandidate{
			name:           node.Name,
			writerEndpoint: endpoint,
			usedBytes:      used,
			availableBytes: available,
		})
	}

	return candidates, nil
}

func nodeIsReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for i := range node.Status.Conditions {
		cond := node.Status.Conditions[i]
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *BackupPolicyReconciler) getLeaseNamespace(policy *snaplanev1alpha1.BackupPolicy) string {
	if r.LeaseNamespace != "" {
		return r.LeaseNamespace
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if policy != nil {
		return policy.Namespace
	}
	return metav1.NamespaceDefault
}

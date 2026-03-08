package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

type conflictOnceClient struct {
	client.Client
	conflictOnPatch int
	patchCalls      int
}

func (c *conflictOnceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patchCalls++
	if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && c.conflictOnPatch > 0 {
		c.conflictOnPatch--
		return apierrors.NewConflict(corev1.Resource("persistentvolumeclaims"), obj.GetName(), fmt.Errorf("injected conflict"))
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestResolveBackupDestinationNodeRetriesPVCPatchConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add coordination scheme: %v", err)
	}

	now := time.Now().UTC()
	renew := metav1.MicroTime{Time: now}
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       "default",
					Name:            "pvc-a",
					ResourceVersion: "1",
				},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-a",
					Labels: map[string]string{
						snaplanev1alpha1.BackupTargetNodeLabelKey: "true",
					},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:   corev1.NodeReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			&coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      writer.LeaseNameForNode("node-a"),
					Annotations: map[string]string{
						snaplanev1alpha1.WriterEndpointAnnotationKey:      "127.0.0.1:9443",
						snaplanev1alpha1.LeaseUsedBytesAnnotationKey:      "100",
						snaplanev1alpha1.LeaseAvailableBytesAnnotationKey: "21474836480",
					},
				},
				Spec: coordinationv1.LeaseSpec{
					RenewTime: &renew,
				},
			},
		).
		Build()

	conflictClient := &conflictOnceClient{
		Client:          baseClient,
		conflictOnPatch: 1,
	}
	reconciler := &BackupPolicyReconciler{
		Client:         conflictClient,
		LeaseNamespace: "default",
	}
	policy := &snaplanev1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "policy-a",
		},
		Spec: snaplanev1alpha1.BackupPolicySpec{
			Source: snaplanev1alpha1.BackupSource{
				PersistentVolumeClaimName: "pvc-a",
			},
		},
	}

	nodeName, err := reconciler.resolveBackupDestinationNode(context.Background(), policy, now)
	if err != nil {
		t.Fatalf("resolveBackupDestinationNode returned error: %v", err)
	}
	if nodeName != "node-a" {
		t.Fatalf("expected node-a, got %q", nodeName)
	}
	if conflictClient.patchCalls < 2 {
		t.Fatalf("expected at least 2 patch calls with retry, got %d", conflictClient.patchCalls)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := baseClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pvc-a"}, &pvc); err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] != "node-a" {
		t.Fatalf("expected assignment annotation node-a, got %q", pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey])
	}
	if pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey] == "" {
		t.Fatalf("expected assignment timestamp annotation to be set")
	}
}

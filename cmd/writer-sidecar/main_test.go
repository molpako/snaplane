package main

import (
	"strings"
	"testing"
	"time"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

func TestAnnotateCASMaintenanceBoundsLastError(t *testing.T) {
	t.Parallel()

	annotations := map[string]string{}
	annotateCASMaintenance(annotations, writer.CASMaintenanceSnapshot{
		LastRunAt:      time.Unix(1, 0),
		LastError:      strings.Repeat("x", maxLeaseAnnotationValue*2),
		RepositoryRuns: 3,
		ReclaimedBytes: 4096,
	})

	value := annotations[snaplanev1alpha1.LeaseCASMaintenanceLastErrorKey]
	if len(value) != maxLeaseAnnotationValue {
		t.Fatalf("expected bounded error length %d, got %d", maxLeaseAnnotationValue, len(value))
	}
	if !strings.HasSuffix(value, truncatedAnnotationMark) {
		t.Fatalf("expected truncation marker, got %q", value[len(value)-len(truncatedAnnotationMark):])
	}
}

func TestAnnotateCASMaintenanceClearsLastError(t *testing.T) {
	t.Parallel()

	annotations := map[string]string{
		snaplanev1alpha1.LeaseCASMaintenanceLastErrorKey: "stale",
	}
	annotateCASMaintenance(annotations, writer.CASMaintenanceSnapshot{
		LastRunAt:      time.Unix(1, 0),
		RepositoryRuns: 1,
	})

	if _, ok := annotations[snaplanev1alpha1.LeaseCASMaintenanceLastErrorKey]; ok {
		t.Fatalf("expected stale error annotation to be cleared")
	}
}

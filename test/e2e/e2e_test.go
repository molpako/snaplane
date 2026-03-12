//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"github.com/molpako/snaplane/test/utils"
)

func TestManagerSmoke(t *testing.T) {
	registerFailureDiagnostics(t)

	feature := features.New("manager smoke").
		Assess("controller-manager deployment becomes available", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			err := wait.For(
				conditions.New(client.Resources()).DeploymentAvailable(managerDeploymentName, namespace),
				wait.WithTimeout(2*time.Minute),
				wait.WithInterval(2*time.Second),
			)
			if err != nil {
				t.Fatalf("controller-manager deployment did not become available: %v", err)
			}
			return ctx
		}).
		Assess("operator CRDs are installed", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			if err := apiextensionsv1.AddToScheme(client.Resources().GetScheme()); err != nil {
				t.Fatalf("add apiextensionsv1 scheme: %v", err)
			}

			for _, crdName := range []string{
				"backups.snaplane.molpako.github.io",
				"backuppolicies.snaplane.molpako.github.io",
			} {
				crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: crdName}}
				err := wait.For(func(ctx context.Context) (bool, error) {
					err := client.Resources().Get(ctx, crdName, "", crd)
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					if err != nil {
						return false, err
					}
					return true, nil
				}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second))
				if err != nil {
					t.Fatalf("CRD %q was not observed: %v", crdName, err)
				}
			}

			return ctx
		}).
		Assess("nightly lane has host-path snapshot metadata stack", func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
			if activeTLSMode != tlsModeCertManager {
				return ctx
			}
			if _, err := utils.Run(exec.Command("kubectl", "get", "storageclass", "csi-hostpath-sc")); err != nil {
				t.Fatalf("host-path storageclass missing: %v", err)
			}
			if _, err := utils.Run(exec.Command("kubectl", "get", "volumesnapshotclass", "csi-hostpath-snapclass")); err != nil {
				t.Fatalf("host-path snapshotclass missing: %v", err)
			}
			if _, err := utils.Run(exec.Command("kubectl", "get", "snapshotmetadataservices.cbt.storage.k8s.io", "hostpath.csi.k8s.io")); err != nil {
				t.Fatalf("snapshotmetadataservice instance missing: %v", err)
			}
			if _, err := utils.Run(exec.Command("kubectl", "get", "service", "csi-snapshot-metadata", "-n", "default")); err != nil {
				t.Fatalf("snapshot metadata service missing: %v", err)
			}
			return ctx
		}).
		Feature()

	testenv.Test(t, feature)
}

func registerFailureDiagnostics(t *testing.T) {
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"get",
			"pods",
			"-A",
			"-o",
			"wide",
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"logs",
			"deployment/"+managerDeploymentName,
			"-n",
			namespace,
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"describe",
			"deployment",
			managerDeploymentName,
			"-n",
			namespace,
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"describe",
			"pod",
			"-n",
			namespace,
			"-l",
			"control-plane=controller-manager",
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"get",
			"secret,certificate,certificaterequest,issuer",
			"-n",
			namespace,
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"get",
			"lease",
			"-A",
			"-o",
			"yaml",
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"get",
			"events",
			"-A",
			"--sort-by=.lastTimestamp",
		))
		dumpCommandOutput(t, exec.Command(
			"kubectl",
			"get",
			"events",
			"-n",
			namespace,
			"--sort-by=.lastTimestamp",
		))
	})
}

func dumpCommandOutput(t *testing.T, cmd *exec.Cmd) {
	t.Helper()

	output, err := utils.Run(cmd)
	if err != nil {
		t.Logf("diagnostic command failed (%s): %v", strings.Join(cmd.Args, " "), err)
		return
	}
	if strings.TrimSpace(output) == "" {
		t.Logf("diagnostic command output was empty (%s)", strings.Join(cmd.Args, " "))
		return
	}

	t.Logf("diagnostic output (%s):\n%s", strings.Join(cmd.Args, " "), output)
}

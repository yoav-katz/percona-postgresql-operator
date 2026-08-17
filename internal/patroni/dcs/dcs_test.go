// Copyright 2021 - 2024 Crunchy Data Solutions, Inc.
//
// SPDX-License-Identifier: Apache-2.0

package dcs

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/percona/percona-postgresql-operator/v2/internal/naming"
	"github.com/percona/percona-postgresql-operator/v2/internal/testing/require"
	"github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// TestApplyStateCleanup covers the one place a backend's requested mutations
// actually reach the API server. Backends only read, so if this is wrong every
// backend is wrong.
func TestApplyStateCleanup(t *testing.T) {
	_, cc := require.Kubernetes2(t)
	require.ParallelCapacity(t, 0)
	ns := require.Namespace(t, cc)
	ctx := context.Background()

	// Not created: ApplyStateCleanup only reads the owner's identity, and a
	// PostgresCluster valid enough for the API server would say nothing extra.
	cluster := new(v1beta1.PostgresCluster)
	cluster.Namespace = ns.Name
	cluster.Name = "apply-cleanup-test"
	cluster.UID = "11111111-2222-3333-4444-555555555555"

	owner := client.FieldOwner(t.Name())

	t.Run("reports Cleared unchanged", func(t *testing.T) {
		recorder := record.NewFakeRecorder(2)

		cleared, err := ApplyStateCleanup(ctx, cc, recorder, owner, cluster,
			StateCleanup{Cleared: true})
		assert.NilError(t, err)
		assert.Assert(t, cleared)
		assert.Equal(t, len(recorder.Events), 0)
	})

	t.Run("records the Warning on the owner", func(t *testing.T) {
		recorder := record.NewFakeRecorder(2)

		_, err := ApplyStateCleanup(ctx, cc, recorder, owner, cluster, StateCleanup{
			Warning: &Warning{Reason: "SomeReason", Message: "some message"},
		})
		assert.NilError(t, err)

		assert.Equal(t, len(recorder.Events), 1)
		assert.Equal(t, <-recorder.Events, "Warning SomeReason some message")
	})

	t.Run("deletes, tolerating NotFound", func(t *testing.T) {
		recorder := record.NewFakeRecorder(2)

		existing := &corev1.Endpoints{ObjectMeta: naming.PatroniLeaderEndpoints(cluster)}
		assert.NilError(t, cc.Create(ctx, existing))

		// The second object was never created; a backend races the API server
		// between reading and having its deletes carried out.
		missing := &corev1.Endpoints{ObjectMeta: naming.PatroniTrigger(cluster)}

		cleared, err := ApplyStateCleanup(ctx, cc, recorder, owner, cluster, StateCleanup{
			Delete: []client.Object{existing, missing},
		})
		assert.NilError(t, err)
		assert.Assert(t, !cleared)

		err = cc.Get(ctx, client.ObjectKeyFromObject(existing), existing)
		assert.Assert(t, apierrors.IsNotFound(err), "expected it deleted, got %v", err)
	})

	t.Run("applies with the owner as controller", func(t *testing.T) {
		recorder := record.NewFakeRecorder(2)

		job := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		job.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "database", Image: "x"}}

		cleared, err := ApplyStateCleanup(ctx, cc, recorder, owner, cluster, StateCleanup{
			Apply: job,
		})
		assert.NilError(t, err)
		assert.Assert(t, !cleared)

		applied := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		assert.NilError(t, cc.Get(ctx, client.ObjectKeyFromObject(applied), applied))

		controller := metav1.GetControllerOf(applied)
		assert.Assert(t, controller != nil, "expected a controller reference")
		assert.Equal(t, controller.Name, cluster.Name)
		assert.Equal(t, controller.UID, cluster.UID)

		// Applying the same object again must not conflict, since the backends
		// hand it over on every pass until they report Cleared.
		again := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		again.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))
		again.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		again.Spec.Template.Spec.Containers = []corev1.Container{{Name: "database", Image: "x"}}

		_, err = ApplyStateCleanup(ctx, cc, recorder, owner, cluster, StateCleanup{Apply: again})
		assert.NilError(t, err)
	})
}

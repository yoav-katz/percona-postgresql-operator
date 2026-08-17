// Copyright 2021 - 2024 Crunchy Data Solutions, Inc.
//
// SPDX-License-Identifier: Apache-2.0

package dcs

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/percona/percona-postgresql-operator/v2/internal/naming"
	"github.com/percona/percona-postgresql-operator/v2/internal/testing/cmp"
	"github.com/percona/percona-postgresql-operator/v2/internal/testing/require"
	"github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// etcdCluster returns a defaulted cluster configured to use etcd for DCS.
func etcdCluster(t *testing.T, endpoints ...string) *v1beta1.PostgresCluster {
	t.Helper()

	cluster := new(v1beta1.PostgresCluster)
	assert.NilError(t, cluster.Default(context.Background(), nil))
	cluster.Namespace = "some-namespace"
	cluster.Name = "cluster-name"

	if len(endpoints) == 0 {
		endpoints = []string{"http://etcd.etcd-cluster.svc:2379"}
	}
	cluster.Spec.Patroni.DCS = &v1beta1.PatroniDCS{
		Type: v1beta1.PatroniDCSTypeEtcd,
		Etcd: &v1beta1.PatroniEtcdSpec{Endpoints: endpoints},
	}

	return cluster
}

func TestForSelectsBackend(t *testing.T) {
	t.Parallel()

	plain := new(v1beta1.PostgresCluster)
	assert.Assert(t, cmp.Equal(For(plain), Backend(kubernetesEndpointsBackend{})),
		"a cluster with no DCS section keeps the Kubernetes backend")

	explicit := new(v1beta1.PostgresCluster)
	explicit.Spec.Patroni = &v1beta1.PatroniSpec{
		DCS: &v1beta1.PatroniDCS{Type: v1beta1.PatroniDCSTypeKubernetes},
	}
	assert.Assert(t, cmp.Equal(For(explicit), Backend(kubernetesEndpointsBackend{})))

	assert.Assert(t, cmp.Equal(For(etcdCluster(t)), Backend(etcdBackend{})))
}

func TestEtcdHostsAndProtocol(t *testing.T) {
	t.Parallel()

	// Patroni wants bare host:port in "hosts" and the scheme in "protocol".
	assert.DeepEqual(t,
		etcdHosts([]string{"https://a.svc:2379", "http://b.svc:2379"}),
		[]string{"a.svc:2379", "b.svc:2379"})

	assert.Equal(t, etcdProtocol([]string{"https://a.svc:2379"}), "https")
	assert.Equal(t, etcdProtocol([]string{"http://a.svc:2379"}), "http")

	// Anything we cannot make sense of falls back rather than emitting a
	// config Patroni would reject.
	assert.DeepEqual(t, etcdHosts([]string{"a.svc:2379"}), []string{"a.svc:2379"})
	assert.Equal(t, etcdProtocol(nil), "http")
	assert.Equal(t, etcdProtocol([]string{"ftp://a.svc:2379"}), "http")
}

func TestEtcdClusterYAML(t *testing.T) {
	t.Parallel()

	t.Run("without TLS", func(t *testing.T) {
		assert.Assert(t, cmp.MarshalMatches((etcdBackend{}).ClusterYAML(etcdCluster(t)), `
etcd3:
  hosts:
  - etcd.etcd-cluster.svc:2379
  protocol: http
postgresql:
  callbacks:
    on_role_change: /opt/crunchy/bin/patroni-role-change.sh
    on_start: /opt/crunchy/bin/patroni-role-change.sh
		`))
	})

	t.Run("with TLS", func(t *testing.T) {
		cluster := etcdCluster(t, "https://etcd.etcd-cluster.svc:2379")
		cluster.Spec.Patroni.DCS.Etcd.TLSSecret = "etcd-tls"

		assert.Assert(t, cmp.MarshalMatches((etcdBackend{}).ClusterYAML(cluster), `
etcd3:
  cacert: /etc/patroni/etcd-tls/ca.crt
  cert: /etc/patroni/etcd-tls/tls.crt
  hosts:
  - etcd.etcd-cluster.svc:2379
  key: /etc/patroni/etcd-tls/tls.key
  protocol: https
postgresql:
  callbacks:
    on_role_change: /opt/crunchy/bin/patroni-role-change.sh
    on_start: /opt/crunchy/bin/patroni-role-change.sh
		`))
	})

	t.Run("no etcd section", func(t *testing.T) {
		cluster := etcdCluster(t)
		cluster.Spec.Patroni.DCS.Etcd = nil

		assert.Assert(t, (etcdBackend{}).ClusterYAML(cluster) == nil)
	})
}

func TestEtcdInstanceYAML(t *testing.T) {
	t.Parallel()

	// Nothing instance-specific: there is no "kubernetes" section to fill in
	// from the downward API.
	assert.Assert(t, (etcdBackend{}).InstanceYAML(etcdCluster(t)) == nil)
}

func TestEtcdPodAdditions(t *testing.T) {
	t.Parallel()

	t.Run("no secrets", func(t *testing.T) {
		additions := (etcdBackend{}).PodAdditions(etcdCluster(t), nil, nil)
		assert.Assert(t, len(additions.Volumes) == 0)
		assert.Assert(t, len(additions.VolumeMounts) == 0)
		assert.Assert(t, len(additions.EnvVars) == 0)
	})

	t.Run("TLS secret", func(t *testing.T) {
		cluster := etcdCluster(t)
		cluster.Spec.Patroni.DCS.Etcd.TLSSecret = "etcd-tls"

		additions := (etcdBackend{}).PodAdditions(cluster, nil, nil)
		assert.Assert(t, cmp.MarshalMatches(additions.Volumes, `
- name: patroni-etcd-tls
  secret:
    defaultMode: 256
    secretName: etcd-tls
		`))
		assert.Assert(t, cmp.MarshalMatches(additions.VolumeMounts, `
- mountPath: /etc/patroni/etcd-tls
  name: patroni-etcd-tls
  readOnly: true
		`))
		assert.Assert(t, len(additions.EnvVars) == 0)
	})

	t.Run("auth secret", func(t *testing.T) {
		cluster := etcdCluster(t)
		cluster.Spec.Patroni.DCS.Etcd.AuthSecret = "etcd-auth"

		additions := (etcdBackend{}).PodAdditions(cluster, nil, nil)
		assert.Assert(t, len(additions.Volumes) == 0)
		assert.Assert(t, cmp.MarshalMatches(additions.EnvVars, `
- name: PATRONI_ETCD3_USERNAME
  valueFrom:
    secretKeyRef:
      key: username
      name: etcd-auth
- name: PATRONI_ETCD3_PASSWORD
  valueFrom:
    secretKeyRef:
      key: password
      name: etcd-auth
		`))
	})
}

// TestEtcdPermissions asserts the etcd backend grants none of the Endpoints
// access the Kubernetes backend needs.
func TestEtcdPermissions(t *testing.T) {
	t.Parallel()

	cluster := etcdCluster(t)
	cluster.Spec.OpenShift = new(bool)
	*cluster.Spec.OpenShift = true

	assert.Assert(t, (etcdBackend{}).Permissions(cluster) == nil)
	assert.Assert(t, (etcdBackend{}).DistributedConfigurationService(cluster) == nil)
}

// TestEtcdServices covers the one part of this backend a user can see
// directly: Patroni cannot maintain Endpoints here, so the leader Service
// selects Pods by the role label the callback script writes. Everything else
// about it -- its name, and the spec.service configuration it carries -- must
// stay identical to the Kubernetes backend, or exposing a cluster would behave
// differently depending on its DCS.
func TestEtcdServices(t *testing.T) {
	t.Parallel()

	cluster := etcdCluster(t)
	cluster.Spec.Port = new(int32)
	*cluster.Spec.Port = 5432

	recorder := record.NewFakeRecorder(2)

	t.Run("leader Service selects the primary", func(t *testing.T) {
		service, err := (etcdBackend{}).LeaderService(cluster, recorder)
		assert.NilError(t, err)

		assert.Equal(t, service.Name, naming.PatroniScope(cluster),
			"clients connect to the same name under either backend")
		assert.DeepEqual(t, service.Spec.Selector, map[string]string{
			naming.LabelCluster: "cluster-name",
			naming.LabelRole:    naming.RolePatroniLeader,
		})
		assert.Equal(t, service.Spec.Type, corev1.ServiceTypeClusterIP)
	})

	t.Run("honors spec.service", func(t *testing.T) {
		exposed := cluster.DeepCopy()
		exposed.Spec.Service = &v1beta1.ServiceSpec{Type: "LoadBalancer"}
		exposed.Spec.Service.LoadBalancerSourceRanges = []string{"10.0.0.0/8"}

		service, err := (etcdBackend{}).LeaderService(exposed, recorder)
		assert.NilError(t, err)
		assert.Equal(t, service.Spec.Type, corev1.ServiceTypeLoadBalancer,
			"spec.service must not be dropped just because Patroni is using etcd")
		assert.DeepEqual(t, service.Spec.LoadBalancerSourceRanges, []string{"10.0.0.0/8"})
	})

	t.Run("primary Service resolves through the leader", func(t *testing.T) {
		leader := new(corev1.Service)
		leader.Spec.ClusterIP = "10.9.8.7"

		spec, subset, err := (etcdBackend{}).PrimaryService(cluster, leader)
		assert.NilError(t, err)
		assert.Equal(t, spec.ClusterIP, corev1.ClusterIPNone)
		assert.Assert(t, subset != nil)
		assert.Equal(t, subset.Addresses[0].IP, "10.9.8.7")
	})

	t.Run("primary Service waits for the leader", func(t *testing.T) {
		_, _, err := (etcdBackend{}).PrimaryService(cluster, nil)
		assert.ErrorContains(t, err, "not available yet")
	})
}

func TestEtcdPollInterval(t *testing.T) {
	t.Parallel()

	cluster := etcdCluster(t)

	// Nothing to poll for before bootstrap.
	assert.Equal(t, (etcdBackend{}).PollInterval(cluster), time.Duration(0))

	// Once bootstrapped, poll at Patroni's own loop_wait, because no watch
	// fires when Patroni records a pending restart only in etcd.
	cluster.Status.Patroni.SystemIdentifier = "1234567890"
	assert.Equal(t, (etcdBackend{}).PollInterval(cluster), 10*time.Second)

	*cluster.Spec.Patroni.SyncPeriodSeconds = 3
	assert.Equal(t, (etcdBackend{}).PollInterval(cluster), 3*time.Second)
}

func TestEtcdObserveAndRestartWithoutInstance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := etcdCluster(t)

	// With no running instance there is nobody to ask, so a ready instance
	// only earns a retry.
	observation, err := (etcdBackend{}).Observe(ctx, nil, cluster, false, nil, nil)
	assert.NilError(t, err)
	assert.Equal(t, observation.SystemIdentifier, "")
	assert.Equal(t, observation.RetryAfter, time.Duration(0))

	observation, err = (etcdBackend{}).Observe(ctx, nil, cluster, true, nil, nil)
	assert.NilError(t, err)
	assert.Equal(t, observation.RetryAfter, time.Second)

	// Already known: no call, no retry.
	cluster.Status.Patroni.SystemIdentifier = "1234567890"
	observation, err = (etcdBackend{}).Observe(ctx, nil, cluster, true, nil, nil)
	assert.NilError(t, err)
	assert.Equal(t, observation.SystemIdentifier, "")
	assert.Equal(t, observation.RetryAfter, time.Duration(0))

	restart, err := (etcdBackend{}).PodRequiresRestart(ctx, cluster, new(corev1.Pod), nil)
	assert.NilError(t, err)
	assert.Assert(t, !restart)
}

func TestPatroniPodHost(t *testing.T) {
	t.Parallel()

	pod := new(corev1.Pod)
	pod.Namespace = "some-namespace"
	pod.Spec.Hostname = "cluster-name-abcd-0"
	pod.Spec.Subdomain = "cluster-name-pods"

	// Must be the name Patroni's REST certificate carries a SAN for.
	assert.Equal(t, patroniPodHost(pod), "cluster-name-abcd-0.cluster-name-pods.some-namespace.svc")
	assert.Equal(t, patroniPodHost(nil), "")
}

// TestEtcdClearState covers the cleanup-Job state machine, including the stale
// restore-ID case: a completed Job from an earlier restore must not be
// mistaken for this restore's work, or the cluster re-bootstraps on top of the
// previous cluster's etcd state.
func TestEtcdClearState(t *testing.T) {
	_, cc := require.Kubernetes2(t)
	require.ParallelCapacity(t, 0)
	ns := require.Namespace(t, cc)
	ctx := context.Background()

	cluster := etcdCluster(t)
	cluster.Namespace = ns.Name
	cluster.Status.StartupInstance = "cluster-name-abcd"

	// The Job mounts the Patroni configuration of a running instance.
	for _, object := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: naming.ClusterConfigMap(cluster)},
		&corev1.ConfigMap{ObjectMeta: naming.InstanceConfigMap(&metav1.ObjectMeta{
			Namespace: ns.Name, Name: "cluster-name-abcd",
		})},
		&corev1.Secret{ObjectMeta: naming.InstanceCertificates(&metav1.ObjectMeta{
			Namespace: ns.Name, Name: "cluster-name-abcd",
		})},
	} {
		assert.NilError(t, cc.Create(ctx, object))
	}

	backend := etcdBackend{}

	// recreateJob replaces the cleanup Job with one in the given terminal
	// state, or still running when terminal is empty. Recreating rather than
	// updating, because Kubernetes does not allow a Job to leave a terminal
	// state once it reaches one.
	recreateJob := func(t *testing.T, restoreID string, terminal batchv1.JobConditionType) {
		t.Helper()

		stale := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		assert.Check(t, client.IgnoreNotFound(cc.Delete(ctx, stale,
			client.PropagationPolicy(metav1.DeletePropagationBackground))))
		assert.NilError(t, wait.PollUntilContextTimeout(ctx, time.Millisecond*50, time.Second*10, true,
			func(ctx context.Context) (bool, error) {
				err := cc.Get(ctx, client.ObjectKeyFromObject(stale), stale)
				return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
			}))

		job := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		job.Annotations = map[string]string{restoreIDAnnotation: restoreID}
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "database", Image: "x"}}
		assert.NilError(t, cc.Create(ctx, job))

		if terminal == "" {
			return
		}

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = finishedJobConditions(terminal)
		if terminal == batchv1.JobComplete {
			job.Status.CompletionTime = &now
			job.Status.Succeeded = 1
		} else {
			job.Status.Failed = 1
		}
		assert.NilError(t, cc.Status().Update(ctx, job))
	}

	t.Run("creates the Job", func(t *testing.T) {
		cleanup, err := backend.ClearState(ctx, cc, cluster, "restore-one")
		assert.NilError(t, err)
		assert.Assert(t, !cleanup.Cleared)
		assert.Assert(t, cleanup.Apply != nil)

		job, ok := cleanup.Apply.(*batchv1.Job)
		assert.Assert(t, ok)
		assert.Equal(t, job.Annotations[restoreIDAnnotation], "restore-one")
		assert.Equal(t, job.Spec.Template.Spec.Containers[0].Command[0], "sh")
		assert.Assert(t, cmp.Contains(job.Spec.Template.Spec.Containers[0].Command[2],
			"patronictl -c '/etc/patroni/~postgres-operator_cluster.yaml' remove 'cluster-name-ha'"))
	})

	t.Run("waits while it runs", func(t *testing.T) {
		recreateJob(t, "restore-one", "")

		cleanup, err := backend.ClearState(ctx, cc, cluster, "restore-one")
		assert.NilError(t, err)
		assert.Assert(t, !cleanup.Cleared)
		assert.Assert(t, cleanup.Apply == nil, "the Job is already there")
		assert.Assert(t, len(cleanup.Delete) == 0)
	})

	t.Run("recreates a failed Job", func(t *testing.T) {
		recreateJob(t, "restore-one", batchv1.JobFailed)

		cleanup, err := backend.ClearState(ctx, cc, cluster, "restore-one")
		assert.NilError(t, err)
		assert.Assert(t, !cleanup.Cleared)
		assert.Equal(t, len(cleanup.Delete), 1)
		assert.Assert(t, cleanup.Warning != nil)
		assert.Equal(t, cleanup.Warning.Reason, "PatroniDCSCleanupFailed")
	})

	t.Run("clears once it succeeds", func(t *testing.T) {
		recreateJob(t, "restore-one", batchv1.JobComplete)

		cleanup, err := backend.ClearState(ctx, cc, cluster, "restore-one")
		assert.NilError(t, err)
		assert.Assert(t, cleanup.Cleared)
	})

	t.Run("a succeeded Job from an earlier restore does not count", func(t *testing.T) {
		// The Job is genuinely Complete -- assert that, or this proves
		// nothing -- but it belongs to a different restore. Reporting Cleared
		// here would skip clearing etcd entirely, and the cluster would
		// re-bootstrap on top of the previous cluster's state.
		live := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		assert.NilError(t, cc.Get(ctx, client.ObjectKeyFromObject(live), live))
		assert.Assert(t, jobSucceeded(live), "precondition: the stale Job must have succeeded")
		assert.Equal(t, live.Annotations[restoreIDAnnotation], "restore-one")

		cleanup, err := backend.ClearState(ctx, cc, cluster, "restore-two")
		assert.NilError(t, err)
		assert.Assert(t, !cleanup.Cleared,
			"a Job left over from restore-one must not satisfy restore-two")
		assert.Equal(t, len(cleanup.Delete), 1, "the stale Job is removed first")
		assert.Assert(t, cleanup.Apply == nil,
			"the replacement waits until the stale Job is gone, since a Job "+
				"cannot be applied over one that is still terminating")

		// Once it is gone, the next pass creates the Job for this restore.
		assert.NilError(t, cc.Delete(ctx, live,
			client.PropagationPolicy(metav1.DeletePropagationBackground)))
		assert.NilError(t, wait.PollUntilContextTimeout(ctx, time.Millisecond*50, time.Second*10, true,
			func(ctx context.Context) (bool, error) {
				err := cc.Get(ctx, client.ObjectKeyFromObject(live), live)
				return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
			}))

		cleanup, err = backend.ClearState(ctx, cc, cluster, "restore-two")
		assert.NilError(t, err)
		assert.Assert(t, !cleanup.Cleared)
		assert.Assert(t, cleanup.Apply != nil)

		fresh, ok := cleanup.Apply.(*batchv1.Job)
		assert.Assert(t, ok)
		assert.Equal(t, fresh.Annotations[restoreIDAnnotation], "restore-two")
	})
}

// TestEtcdClearLeaderMatchesClearState asserts the delegation: "patronictl
// remove" is the only lever etcd has, so clearing the leader lock clears
// everything ClearState does.
func TestEtcdClearLeaderMatchesClearState(t *testing.T) {
	_, cc := require.Kubernetes2(t)
	require.ParallelCapacity(t, 0)
	ns := require.Namespace(t, cc)
	ctx := context.Background()

	cluster := etcdCluster(t)
	cluster.Namespace = ns.Name
	cluster.Status.StartupInstance = "cluster-name-abcd"

	for _, object := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: naming.ClusterConfigMap(cluster)},
		&corev1.ConfigMap{ObjectMeta: naming.InstanceConfigMap(&metav1.ObjectMeta{
			Namespace: ns.Name, Name: "cluster-name-abcd",
		})},
		&corev1.Secret{ObjectMeta: naming.InstanceCertificates(&metav1.ObjectMeta{
			Namespace: ns.Name, Name: "cluster-name-abcd",
		})},
	} {
		assert.NilError(t, cc.Create(ctx, object))
	}

	backend := etcdBackend{}

	leader, err := backend.ClearLeader(ctx, cc, cluster, "restore-one")
	assert.NilError(t, err)

	state, err := backend.ClearState(ctx, cc, cluster, "restore-one")
	assert.NilError(t, err)

	assert.Equal(t, leader.Cleared, state.Cleared)
	assert.Equal(t, len(leader.Delete), len(state.Delete))
	assert.DeepEqual(t, leader.Apply, state.Apply)
}

// TestEtcdDeleteGivesUp asserts teardown never blocks on etcd: leftover keys
// in a store the operator does not own beat a cluster that cannot be deleted.
func TestEtcdDeleteGivesUp(t *testing.T) {
	_, cc := require.Kubernetes2(t)
	require.ParallelCapacity(t, 0)
	ns := require.Namespace(t, cc)
	ctx := context.Background()

	cluster := etcdCluster(t)
	cluster.Namespace = ns.Name
	backend := etcdBackend{}

	t.Run("never bootstrapped", func(t *testing.T) {
		cleanup, err := backend.Delete(ctx, cc, cluster)
		assert.NilError(t, err)
		assert.Assert(t, cleanup.Cleared, "nothing was ever written to etcd")
		assert.Assert(t, cleanup.Apply == nil)
	})

	cluster.Status.Patroni.SystemIdentifier = "1234567890"

	t.Run("missing instance config does not block deletion", func(t *testing.T) {
		cleanup, err := backend.Delete(ctx, cc, cluster)
		assert.NilError(t, err)
		assert.Assert(t, cleanup.Cleared,
			"a cluster must still be deletable when its config is already gone")
		assert.Assert(t, cleanup.Warning != nil)
		assert.Equal(t, cleanup.Warning.Reason, "PatroniDCSCleanupSkipped")
	})

	t.Run("a failed Job does not block deletion", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "database", Image: "x"}}
		assert.NilError(t, cc.Create(ctx, job))

		now := metav1.Now()
		job.Status.Conditions = finishedJobConditions(batchv1.JobFailed)
		job.Status.StartTime = &now
		assert.NilError(t, cc.Status().Update(ctx, job))

		cleanup, err := backend.Delete(ctx, cc, cluster)
		assert.NilError(t, err)
		assert.Assert(t, cleanup.Cleared)
		assert.Equal(t, cleanup.Warning.Reason, "PatroniDCSCleanupFailed")
	})
}

// finishedJobConditions returns the conditions a finished Job must carry.
// Since Kubernetes 1.31 the API server rejects a terminal condition set on its
// own: Failed needs FailureTarget first, and Complete needs SuccessCriteriaMet.
func finishedJobConditions(terminal batchv1.JobConditionType) []batchv1.JobCondition {
	precursor := batchv1.JobSuccessCriteriaMet
	if terminal == batchv1.JobFailed {
		precursor = batchv1.JobFailureTarget
	}

	return []batchv1.JobCondition{
		{Type: precursor, Status: corev1.ConditionTrue},
		{Type: terminal, Status: corev1.ConditionTrue},
	}
}

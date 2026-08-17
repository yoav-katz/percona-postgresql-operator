// Copyright 2021 - 2024 Crunchy Data Solutions, Inc.
//
// SPDX-License-Identifier: Apache-2.0

package dcs

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/percona/percona-postgresql-operator/v2/internal/config"
	"github.com/percona/percona-postgresql-operator/v2/internal/initialize"
	"github.com/percona/percona-postgresql-operator/v2/internal/logging"
	"github.com/percona/percona-postgresql-operator/v2/internal/naming"
	"github.com/percona/percona-postgresql-operator/v2/internal/patroni"
	"github.com/percona/percona-postgresql-operator/v2/internal/postgres"
	"github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// etcdBackend uses an external etcd cluster as Patroni's DCS.
//
// Patroni writes nothing to Kubernetes under this backend, which has two
// consequences the rest of this file exists to handle. First, nothing labels
// the Pods, so a callback script does it (see roleChangeCallback). Second,
// none of the operator's watches fire on Patroni state changes, so runtime
// state is read from Patroni's REST API on a poll (see Observe, PollInterval).
type etcdBackend struct{}

const (
	// etcdTLSVolume is the name of the Volume holding the client certificates
	// Patroni presents to etcd.
	etcdTLSVolume = "patroni-etcd-tls"

	// etcdTLSDirectory is where etcdTLSVolume is mounted, relative to
	// Patroni's configuration directory.
	etcdTLSDirectory = "etcd-tls"

	// roleChangeCallback is the script Patroni runs on start and on every role
	// change. It restores the Pod label and status annotation that Patroni
	// itself maintains when using Kubernetes for DCS, and that the rest of the
	// operator (Service routing, IsPrimary, IsWritable) depends on.
	roleChangeCallback = "/opt/crunchy/bin/patroni-role-change.sh"

	// restoreIDAnnotation records which restore a cleanup Job was created for,
	// so a Job left behind by a previous restore is not mistaken for this
	// restore's work. See ClearState.
	restoreIDAnnotation = "postgres-operator.crunchydata.com/patroni-dcs-cleanup-restore-id"
)

// etcdSpec returns cluster's etcd settings, or nil when they are absent.
// The CRD requires them when the DCS type is etcd, so nil means a hand-edited
// or partially-defaulted object; every caller treats it as "nothing to do".
func etcdSpec(cluster *v1beta1.PostgresCluster) *v1beta1.PatroniEtcdSpec {
	if dcs := cluster.GetDCS(); dcs != nil {
		return dcs.Etcd
	}
	return nil
}

// etcdHosts strips the URL scheme from each endpoint, since Patroni's
// "etcd3.hosts" takes bare host:port strings and carries the scheme
// separately in "etcd3.protocol".
func etcdHosts(endpoints []string) []string {
	hosts := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Host == "" {
			hosts = append(hosts, ep)
			continue
		}
		hosts = append(hosts, u.Host)
	}
	return hosts
}

// etcdProtocol returns the URL scheme of the first endpoint. The CRD requires
// every endpoint to use the same scheme, so the first one decides. Anything
// unparseable falls back to "http".
func etcdProtocol(endpoints []string) string {
	if len(endpoints) == 0 {
		return "http"
	}
	u, err := url.Parse(endpoints[0])
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "http"
	}
	return u.Scheme
}

func (etcdBackend) ClusterYAML(cluster *v1beta1.PostgresCluster) map[string]any {
	etcd := etcdSpec(cluster)
	if etcd == nil {
		return nil
	}

	// These values cannot change during the cluster's lifetime.
	etcd3 := map[string]any{
		"hosts":    etcdHosts(etcd.Endpoints),
		"protocol": etcdProtocol(etcd.Endpoints),
	}
	if etcd.TLSSecret != "" {
		dir := path.Join(patroni.ConfigDirectory, etcdTLSDirectory)
		etcd3["cacert"] = path.Join(dir, "ca.crt")
		etcd3["cert"] = path.Join(dir, "tls.crt")
		etcd3["key"] = path.Join(dir, "tls.key")
	}

	// Patroni does not touch Pod labels under this backend, so the callback
	// script maintains them instead. "on_role_change" covers failovers and
	// "on_start" covers restarts where the role does not change, which is what
	// re-labels a Pod that comes back as a replica.
	return map[string]any{
		"etcd3": etcd3,
		"postgresql": map[string]any{
			"callbacks": map[string]any{
				"on_start":       roleChangeCallback,
				"on_role_change": roleChangeCallback,
			},
		},
	}
}

func (etcdBackend) InstanceYAML(*v1beta1.PostgresCluster) map[string]any {
	return nil
}

func (etcdBackend) PodAdditions(
	cluster *v1beta1.PostgresCluster, _ *corev1.Service, _ []corev1.Container,
) patroni.PodAdditions {
	var additions patroni.PodAdditions

	etcd := etcdSpec(cluster)
	if etcd == nil {
		return additions
	}

	if etcd.TLSSecret != "" {
		additions.Volumes = append(additions.Volumes, corev1.Volume{
			Name: etcdTLSVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  etcd.TLSSecret,
					DefaultMode: new(int32(0o400)),
				},
			},
		})
		additions.VolumeMounts = append(additions.VolumeMounts, corev1.VolumeMount{
			Name:      etcdTLSVolume,
			MountPath: path.Join(patroni.ConfigDirectory, etcdTLSDirectory),
			ReadOnly:  true,
		})
	}

	if etcd.AuthSecret != "" {
		// Patroni reads PATRONI_ETCD3_USERNAME and PATRONI_ETCD3_PASSWORD as
		// overrides for etcd3.username and etcd3.password in the config, which
		// keeps the credentials out of the ConfigMap.
		additions.EnvVars = append(additions.EnvVars,
			corev1.EnvVar{
				Name: "PATRONI_ETCD3_USERNAME",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: etcd.AuthSecret},
						Key:                  "username",
					},
				},
			},
			corev1.EnvVar{
				Name: "PATRONI_ETCD3_PASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: etcd.AuthSecret},
						Key:                  "password",
					},
				},
			},
		)
	}

	return additions
}

// Permissions are none: Patroni keeps its state in etcd, so it needs no
// Endpoints access and does not try to create the "{scope}-config" Service.
// The Pod "patch" permission the role-change callback needs is already granted
// unconditionally by patroni.Permissions.
func (etcdBackend) Permissions(*v1beta1.PostgresCluster) []rbacv1.PolicyRule {
	return nil
}

// DistributedConfigurationService is nil: the distributed configuration lives
// in etcd, so there is no Endpoints object for a Service to protect.
func (etcdBackend) DistributedConfigurationService(*v1beta1.PostgresCluster) *corev1.Service {
	return nil
}

// LeaderService returns the same Service the Kubernetes backend does, so that
// the name clients connect to and the cluster.Spec.Service configuration
// (type, nodePort, traffic policies, source ranges) behave identically under
// both backends. Only the way it finds the leader differs: Patroni cannot
// maintain Endpoints here, so the Service selects Pods that the role-change
// callback has labelled as the primary.
func (etcdBackend) LeaderService(
	cluster *v1beta1.PostgresCluster, recorder record.EventRecorder,
) (*corev1.Service, error) {
	return leaderService(cluster, recorder, map[string]string{
		naming.LabelCluster: cluster.Name,
		naming.LabelRole:    naming.RolePatroniLeader,
	})
}

func (etcdBackend) PrimaryService(
	cluster *v1beta1.PostgresCluster, leader *corev1.Service,
) (corev1.ServiceSpec, *corev1.EndpointSubset, error) {
	return primaryServiceViaLeader(cluster, leader)
}

// Observe asks a running instance for the system identifier. Patroni writes no
// "initialize" key to Kubernetes here, but its monitoring REST API reports the
// same value under any DCS backend.
func (etcdBackend) Observe(
	ctx context.Context, _ client.Client, cluster *v1beta1.PostgresCluster,
	readyInstance bool, pod *corev1.Pod, exec patroni.Executor,
) (Observation, error) {
	var observation Observation

	// Already known, or no running instance to ask.
	if cluster.Status.Patroni.SystemIdentifier != "" || exec == nil || pod == nil {
		if cluster.Status.Patroni.SystemIdentifier == "" && readyInstance {
			observation.RetryAfter = time.Second
		}
		return observation, nil
	}

	status, err := exec.GetMemberStatus(ctx, patroniPodHost(pod), cluster.Spec.Patroni.GetPort())
	if err == nil && status.DatabaseSystemIdentifier != "" {
		observation.SystemIdentifier = status.DatabaseSystemIdentifier
	} else if readyInstance {
		logging.FromContext(ctx).Info("detected ready instance but no system identifier yet")
		observation.RetryAfter = time.Second
	}

	// A failed REST call is not an error worth failing the reconcile over; the
	// retry above will pick it up.
	return observation, nil
}

// PollInterval matches Patroni's own "loop_wait", so state Patroni only
// records in etcd -- most importantly a pending restart -- is noticed about as
// quickly as it changes. Before bootstrap there is nothing to poll for.
func (etcdBackend) PollInterval(cluster *v1beta1.PostgresCluster) time.Duration {
	if !patroni.ClusterBootstrapped(cluster) {
		return 0
	}
	// cluster.Default() guarantees SyncPeriodSeconds is set by the time any
	// reconcile reaches this.
	return time.Duration(*cluster.Spec.Patroni.SyncPeriodSeconds) * time.Second
}

// PodRequiresRestart asks Patroni's monitoring REST API directly, since the
// "status" annotation the Kubernetes backend reads is never written here.
func (etcdBackend) PodRequiresRestart(
	ctx context.Context, cluster *v1beta1.PostgresCluster,
	pod *corev1.Pod, exec patroni.Executor,
) (bool, error) {
	if exec == nil {
		return false, nil
	}

	restart, err := exec.PodRequiresRestart(ctx, patroniPodHost(pod), cluster.Spec.Patroni.GetPort())
	if err != nil {
		// Treat an unreachable instance as "no restart needed" rather than
		// failing the reconcile: the next poll will ask again.
		logging.FromContext(ctx).Error(err, "failed to check Patroni pending_restart",
			"namespace", pod.Namespace, "name", pod.Name)
		return false, nil
	}
	return restart, nil
}

// patroniPodHost returns pod's own stable DNS name, which is what Patroni's
// REST API certificate carries a SAN for. Calls made inside the Pod cannot use
// "localhost", because Patroni does not certify it.
func patroniPodHost(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return fmt.Sprintf("%s.%s.%s.svc", pod.Spec.Hostname, pod.Spec.Subdomain, pod.Namespace)
}

// ClearState runs "patronictl remove" against etcd as a one-shot Job, which
// deletes the leader lock, the member keys, and the "initialize" key.
//
// The Job is keyed on restoreID. A completed Job from an earlier restore is
// indistinguishable from a completed Job for this one unless we check, and
// treating a stale one as success would re-bootstrap the cluster on top of the
// previous cluster's etcd state.
func (b etcdBackend) ClearState(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster, restoreID string,
) (StateCleanup, error) {
	var cleanup StateCleanup

	existing, err := b.cleanupJob(ctx, cli, cluster)
	if err != nil {
		return cleanup, err
	}

	switch {
	case existing != nil && existing.Annotations[restoreIDAnnotation] != restoreID:
		// Left over from a different restore. Only delete it here; the next
		// pass creates the replacement, once this one is actually gone. A Job
		// cannot be applied over one that is still terminating.
		cleanup.Delete = append(cleanup.Delete, existing)

	case existing == nil:
		job, err := b.newCleanupJob(ctx, cli, cluster, restoreID, 0)
		if err != nil {
			return cleanup, err
		}
		cleanup.Apply = job

	case jobFailed(existing):
		cleanup.Delete = append(cleanup.Delete, existing)
		cleanup.Warning = &Warning{
			Reason:  "PatroniDCSCleanupFailed",
			Message: "patronictl remove failed; retrying",
		}

	case jobSucceeded(existing):
		cleanup.Cleared = true
	}

	return cleanup, nil
}

// ClearLeader clears the leader lock the only way etcd can: "patronictl
// remove", which also drops the member keys and the "initialize" key. A
// cluster restored under this backend therefore re-registers itself with etcd
// from its restored data directory.
func (b etcdBackend) ClearLeader(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster, restoreID string,
) (StateCleanup, error) {
	return b.ClearState(ctx, cli, cluster, restoreID)
}

// Delete clears cluster's state from etcd on teardown, using the same Job as
// ClearState. Unlike ClearState it gives up rather than retrying forever: a
// cluster that cannot finish deleting is a worse outcome than leftover keys in
// an etcd the operator does not own.
func (b etcdBackend) Delete(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
) (StateCleanup, error) {
	var cleanup StateCleanup

	// Nothing was ever written to etcd under this scope.
	if !patroni.ClusterBootstrapped(cluster) {
		return StateCleanup{Cleared: true}, nil
	}

	existing, err := b.cleanupJob(ctx, cli, cluster)
	if err != nil {
		return cleanup, err
	}

	switch {
	case existing == nil:
		// The Job is owned by the cluster, so it is garbage collected once the
		// finalizer is released; there is no need to delete it here.
		job, err := b.newCleanupJob(ctx, cli, cluster, "", 2)
		if err != nil {
			// The instance config this Job needs may already be gone. Do not
			// block deletion on it.
			logging.FromContext(ctx).Error(err, "cannot build Patroni DCS cleanup Job; "+
				"leaving cluster state in etcd")
			return StateCleanup{
				Cleared: true,
				Warning: &Warning{
					Reason:  "PatroniDCSCleanupSkipped",
					Message: "could not run patronictl remove: " + err.Error(),
				},
			}, nil
		}
		cleanup.Apply = job

	case jobFailed(existing):
		cleanup.Cleared = true
		cleanup.Warning = &Warning{
			Reason:  "PatroniDCSCleanupFailed",
			Message: "patronictl remove failed; cluster state remains in etcd",
		}

	case jobSucceeded(existing):
		cleanup.Cleared = true
	}

	return cleanup, nil
}

// cleanupJob returns the existing "patronictl remove" Job, or nil.
func (etcdBackend) cleanupJob(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: naming.PatroniDCSCleanupJob(cluster)}
	err := cli.Get(ctx, client.ObjectKeyFromObject(job), job)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return job, nil
}

// newCleanupJob builds the Job that runs "patronictl remove" against etcd.
func (b etcdBackend) newCleanupJob(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
	restoreID string, backoffLimit int32,
) (*batchv1.Job, error) {
	clusterConfigMap, instanceConfigMap, instanceCertificates, err :=
		patroniConfigFor(ctx, cli, cluster)
	if err != nil {
		return nil, err
	}

	meta := naming.PatroniDCSCleanupJob(cluster)
	meta.Labels = naming.Merge(
		cluster.Spec.Metadata.GetLabelsOrNil(),
		naming.WithPerconaLabels(
			naming.PatroniDCSCleanupJobLabels(cluster.Name),
			cluster.Name, "", cluster.Labels[naming.LabelVersion]),
	)
	meta.Annotations = map[string]string{restoreIDAnnotation: restoreID}

	// This Job is part of the in-place restore, so it reuses the restore's
	// resources and scheduling configuration.
	var resources corev1.ResourceRequirements
	restore := cluster.Spec.Backups.PGBackRest.Restore
	if restore != nil {
		resources = restore.Resources
	}

	job := &batchv1.Job{ObjectMeta: meta}
	job.Spec = batchv1.JobSpec{
		BackoffLimit: new(backoffLimit),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: meta.Labels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:            naming.ContainerDatabase,
					Image:           config.PostgresContainerImage(cluster),
					ImagePullPolicy: cluster.Spec.ImagePullPolicy,
					SecurityContext: initialize.RestrictedSecurityContext(cluster.CompareVersion("2.5.0") >= 0),
					Resources:       resources,
				}},
				RestartPolicy:                corev1.RestartPolicyNever,
				AutomountServiceAccountToken: new(false),
				EnableServiceLinks:           new(false),
				ImagePullSecrets:             cluster.Spec.ImagePullSecrets,
				SecurityContext:              postgres.PodSecurityContext(cluster),
			},
		},
	}

	if restore != nil {
		job.Spec.Template.Spec.Affinity = restore.Affinity
		job.Spec.Template.Spec.Tolerations = restore.Tolerations
		if restore.PriorityClassName != nil {
			job.Spec.Template.Spec.PriorityClassName = *restore.PriorityClassName
		}
	}

	patroni.RemoveClusterPod(cluster, clusterConfigMap, instanceConfigMap, instanceCertificates,
		b.PodAdditions(cluster, nil, nil), &job.Spec.Template)

	job.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))
	return job, nil
}

// patroniConfigFor gathers the ConfigMaps and Secret a Patroni process needs
// to talk to cluster's DCS. Any instance's config will do, since the parts
// that matter here -- scope, etcd connection, ctl certificates -- are the same
// on all of them; the startup instance is preferred only because it is the one
// the restore path already knows about.
func patroniConfigFor(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
) (clusterConfigMap, instanceConfigMap *corev1.ConfigMap, certificates *corev1.Secret, err error) {
	clusterConfigMap = &corev1.ConfigMap{}
	if err = cli.Get(ctx,
		naming.AsObjectKey(naming.ClusterConfigMap(cluster)), clusterConfigMap); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	instance := cluster.Status.StartupInstance
	if instance == "" {
		if instance, err = anyInstanceName(ctx, cli, cluster); err != nil {
			return nil, nil, nil, err
		}
	}
	instanceMeta := metav1.ObjectMeta{Namespace: cluster.Namespace, Name: instance}

	instanceConfigMap = &corev1.ConfigMap{}
	if err = cli.Get(ctx,
		naming.AsObjectKey(naming.InstanceConfigMap(&instanceMeta)), instanceConfigMap); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	certificates = &corev1.Secret{}
	if err = cli.Get(ctx,
		naming.AsObjectKey(naming.InstanceCertificates(&instanceMeta)), certificates); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	return clusterConfigMap, instanceConfigMap, certificates, nil
}

// anyInstanceName returns the name of any instance of cluster, derived from
// its config ConfigMaps. Used when the cluster has no startup instance
// recorded, e.g. during teardown.
func anyInstanceName(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
) (string, error) {
	selector, err := naming.AsSelector(naming.ClusterInstances(cluster.Name))
	if err != nil {
		return "", errors.WithStack(err)
	}

	configMaps := &corev1.ConfigMapList{}
	if err := cli.List(ctx, configMaps,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return "", errors.WithStack(err)
	}

	for i := range configMaps.Items {
		if name := configMaps.Items[i].Labels[naming.LabelInstance]; name != "" {
			return name, nil
		}
	}

	return "", errors.New("no Patroni instance configuration found")
}

// jobFailed reports whether job has given up. The controller package has an
// identical helper; duplicating three lines here is cheaper than exporting it
// and having internal/patroni/dcs depend on the controller.
func jobFailed(job *batchv1.Job) bool {
	return jobHasCondition(job, batchv1.JobFailed)
}

// jobSucceeded reports whether job completed successfully.
func jobSucceeded(job *batchv1.Job) bool {
	return jobHasCondition(job, batchv1.JobComplete)
}

func jobHasCondition(job *batchv1.Job, want batchv1.JobConditionType) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == want {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

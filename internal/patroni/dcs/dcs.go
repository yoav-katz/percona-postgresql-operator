// Copyright 2021 - 2024 Crunchy Data Solutions, Inc.
//
// SPDX-License-Identifier: Apache-2.0

// Package dcs owns everything specific to the distributed configuration
// store (DCS) backend Patroni uses. Generic Patroni logic lives in
// internal/patroni; DCS-specific behavior belongs here.
//
// Backends never perform writes themselves. They read through the
// client.Client they are handed and return the objects the caller should
// apply or delete, the same way they already return Services for the caller
// to own and apply. That keeps ownership, server-side apply, and event
// recording in one place: ApplyStateCleanup.
package dcs

import (
	"context"
	"reflect"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/percona/percona-postgresql-operator/v2/internal/initialize"
	"github.com/percona/percona-postgresql-operator/v2/internal/naming"
	"github.com/percona/percona-postgresql-operator/v2/internal/patroni"
	"github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// Backend describes the behavior a Patroni DCS backend must provide.
// Implementations are stateless; any dependency they need (a client, an
// executor, an event recorder) is passed as a parameter.
type Backend interface {
	// --- Patroni configuration additions ---

	// ClusterYAML returns top-level Patroni config keys owned by this
	// backend (e.g. "kubernetes"), merged into the cluster-wide config.
	ClusterYAML(cluster *v1beta1.PostgresCluster) map[string]any

	// InstanceYAML returns top-level Patroni config keys owned by this
	// backend, merged into each instance's config.
	InstanceYAML(cluster *v1beta1.PostgresCluster) map[string]any

	// --- Pod additions ---

	// PodAdditions returns the Volumes, VolumeMounts, and environment
	// variables this backend needs in any Pod running Patroni or patronictl.
	PodAdditions(cluster *v1beta1.PostgresCluster,
		leaderService *corev1.Service, podContainers []corev1.Container) patroni.PodAdditions

	// --- RBAC ---

	// Permissions returns backend-specific RBAC rules for Patroni's
	// ServiceAccount, in addition to the generic rules patroni.Permissions
	// always grants.
	Permissions(cluster *v1beta1.PostgresCluster) []rbacv1.PolicyRule

	// --- Kubernetes objects this backend owns for its own bookkeeping ---

	// DistributedConfigurationService returns the Service this backend needs
	// to protect its distributed-configuration objects, or nil when it owns
	// no such object.
	DistributedConfigurationService(cluster *v1beta1.PostgresCluster) *corev1.Service

	// LeaderService returns the Service that resolves to the elected Patroni
	// leader, or nil when this backend has no such Service. How it resolves
	// is the backend's business: Patroni may maintain the Endpoints itself,
	// or the Service may select Pods by role label.
	//
	// This is the Service that carries the user's cluster.Spec.Service
	// configuration, so a backend that returns nil here must apply that
	// configuration somewhere else or silently drop it.
	LeaderService(cluster *v1beta1.PostgresCluster,
		recorder record.EventRecorder) (*corev1.Service, error)

	// PrimaryService returns the ServiceSpec and, if this backend needs the
	// operator to manage them itself, EndpointSubset that route traffic to
	// cluster's current PostgreSQL primary. leader is this backend's own
	// LeaderService result (nil if it has none, or it hasn't been created
	// yet). endpointSubset is nil when the ServiceSpec's Selector already
	// routes traffic on its own, in which case the operator manages no
	// Endpoints for this Service.
	PrimaryService(cluster *v1beta1.PostgresCluster, leader *corev1.Service) (
		spec corev1.ServiceSpec, endpointSubset *corev1.EndpointSubset, err error)

	// --- Runtime observation ---

	// Observe reports what this backend can tell us about Patroni's runtime
	// state this reconcile. readyInstance tells the backend whether any
	// instance is currently Ready, since "not bootstrapped yet" vs.
	// "bootstrapped but our signal is delayed" needs different handling and
	// is backend-specific policy. pod is a running instance and exec runs
	// commands in its database container; both are nil when no instance is
	// running.
	Observe(ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
		readyInstance bool, pod *corev1.Pod, exec patroni.Executor) (Observation, error)

	// PollInterval is how often the caller must reconcile to notice changes
	// this backend cannot deliver as watch events. It returns zero when the
	// backend's state changes do arrive as watch events.
	//
	// This is deliberately separate from Observation.RetryAfter: that one
	// says a single observation wasn't available yet, while this one is a
	// standing property of the backend.
	PollInterval(cluster *v1beta1.PostgresCluster) time.Duration

	// PodRequiresRestart reports whether PostgreSQL in pod has pending
	// parameter changes that require a restart. exec runs commands in pod's
	// database container.
	PodRequiresRestart(ctx context.Context, cluster *v1beta1.PostgresCluster,
		pod *corev1.Pod, exec patroni.Executor) (bool, error)

	// --- State teardown ---

	// ClearState removes the DCS state that would stop cluster from
	// re-bootstrapping during an in-place restore: the leader lock, the
	// member keys, and the record that the cluster was ever initialized.
	//
	// restoreID identifies the restore being prepared for, so a backend can
	// tell its own leftovers from a previous restore apart from work it has
	// done for this one. Clearing may take several reconciles; the caller
	// performs the returned mutations and calls again until Cleared.
	//
	// This must only be called once Patroni is stopped everywhere, or a live
	// instance will write its state straight back.
	ClearState(ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
		restoreID string) (StateCleanup, error)

	// ClearLeader removes the stale leader lock, so a cluster whose data has
	// been replaced underneath it can elect a leader against that data. Unlike
	// ClearState it does not set out to make the cluster forget it was ever
	// initialized, so a restore that keeps the cluster's identity can use it.
	//
	// A backend with no way to express "the lock alone" clears more; the etcd
	// backend has only "patronictl remove", so under it a cluster does forget.
	// restoreID keys any Job the backend needs, as in ClearState. Like
	// ClearState it may take several reconciles, the caller performs the
	// returned mutations and calls again until Cleared, and it must only be
	// called once Patroni is stopped everywhere.
	ClearLeader(ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
		restoreID string) (StateCleanup, error)

	// Delete removes any backend-owned state during cluster teardown. Like
	// ClearState it may take several reconciles, and unlike ClearState it
	// must eventually report Cleared even when it fails: a cluster that
	// cannot finish deleting is worse than a DCS with leftovers in it.
	Delete(ctx context.Context, cli client.Client,
		cluster *v1beta1.PostgresCluster) (StateCleanup, error)
}

// Observation is what a backend learned about Patroni's runtime state on a
// single reconcile pass.
type Observation struct {
	// SystemIdentifier is "" when not yet known.
	SystemIdentifier string

	// RetryAfter asks the caller to observe again soon because this
	// observation was not available yet. It is zero when the observation is
	// complete. This describes the observation, not a polling schedule; see
	// Backend.PollInterval for that.
	RetryAfter time.Duration
}

// StateCleanup is a backend's report on clearing Patroni's DCS state, plus
// the work it needs the caller to carry out before asking again.
type StateCleanup struct {
	// Cleared is true when none of Patroni's DCS state remains.
	Cleared bool

	// Apply, when non-nil, is server-side applied by the caller with the
	// owner it passes to ApplyStateCleanup as its controller owner.
	Apply client.Object

	// Delete are deleted by the caller, ignoring NotFound.
	Delete []client.Object

	// Warning, when non-nil, is recorded as a Warning event on that owner.
	Warning *Warning
}

// Warning is an event a backend wants recorded.
type Warning struct {
	Reason, Message string
}

// For selects the DCS backend for cluster.
func For(cluster *v1beta1.PostgresCluster) Backend {
	if cluster.DCSType() == v1beta1.PatroniDCSTypeEtcd {
		return etcdBackend{}
	}
	return kubernetesEndpointsBackend{}
}

// ApplyStateCleanup carries out the work a backend asked for and reports
// whether the backend considers its state cleared. Backends only read; every
// write they want goes through here so ownership, apply, and event recording
// stay in one place.
//
// owner both controls anything applied and receives any Warning event, so it
// should be whichever object the caller is reconciling.
func ApplyStateCleanup(
	ctx context.Context, cli client.Client, recorder record.EventRecorder,
	fieldOwner client.FieldOwner, owner client.Object, cleanup StateCleanup,
) (bool, error) {
	if warning := cleanup.Warning; warning != nil {
		recorder.Eventf(owner, corev1.EventTypeWarning, warning.Reason, "%s", warning.Message)
	}

	for _, object := range cleanup.Delete {
		if err := cli.Delete(ctx, object,
			client.PropagationPolicy(metav1.DeletePropagationBackground),
		); client.IgnoreNotFound(err) != nil {
			return false, errors.WithStack(err)
		}
	}

	if cleanup.Apply != nil {
		if err := controllerutil.SetControllerReference(
			owner, cleanup.Apply, cli.Scheme()); err != nil {
			return false, errors.WithStack(err)
		}
		if err := applyObject(ctx, cli, fieldOwner, cleanup.Apply); err != nil {
			return false, err
		}
	}

	return cleanup.Cleared, nil
}

// applyObject server-side applies object as fieldOwner, forcing conflicts.
// The patch is generated by diffing object against its zero value, matching
// postgrescluster.Reconciler.apply so this path is unchanged for its existing
// caller. That function's Service workaround is not reproduced here:
// StateCleanup's Apply is only ever a Job.
func applyObject(
	ctx context.Context, cli client.Client,
	fieldOwner client.FieldOwner, object client.Object,
) error {
	zero := reflect.New(reflect.TypeOf(object).Elem()).Interface()
	data, err := client.MergeFrom(zero.(client.Object)).Data(object)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(cli.Patch(ctx, object,
		client.RawPatch(client.Apply.Type(), data), fieldOwner, client.ForceOwnership))
}

// --- Shared Service construction ---
//
// Both backends expose the leader through a Service of the same name carrying
// the same user configuration. They differ only in how that Service finds the
// leader, so the construction lives here and each backend supplies a selector.

// leaderService builds the Service that resolves to the Patroni leader.
// A nil selector leaves the Service without one, so that something else (i.e.
// Patroni) can manage its Endpoints.
func leaderService(
	cluster *v1beta1.PostgresCluster, recorder record.EventRecorder,
	selector map[string]string,
) (*corev1.Service, error) {
	service := &corev1.Service{ObjectMeta: naming.PatroniLeaderEndpoints(cluster)}
	service.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))

	service.Annotations = naming.Merge(
		cluster.Spec.Metadata.GetAnnotationsOrNil(),
	)
	service.Labels = naming.Merge(
		cluster.Spec.Metadata.GetLabelsOrNil(),
	)

	if spec := cluster.Spec.Service; spec != nil {
		service.Annotations = naming.Merge(service.Annotations,
			spec.Metadata.GetAnnotationsOrNil())
		service.Labels = naming.Merge(service.Labels,
			spec.Metadata.GetLabelsOrNil())
	}

	// add our labels last so they aren't overwritten
	service.Labels = naming.Merge(service.Labels,
		naming.WithPerconaLabels(map[string]string{ // K8SPG-430
			naming.LabelCluster: cluster.Name,
			naming.LabelPatroni: naming.PatroniScope(cluster),
		}, cluster.Name, "", cluster.Labels[naming.LabelVersion]))

	service.Spec.Selector = selector

	// The TargetPort must be the name (not the number) of the PostgreSQL
	// ContainerPort. This name allows the port number to differ between
	// instances, which can happen during a rolling update.
	servicePort := corev1.ServicePort{
		Name:       naming.PortPostgreSQL,
		Port:       *cluster.Spec.Port,
		Protocol:   corev1.ProtocolTCP,
		TargetPort: intstr.FromString(naming.PortPostgreSQL),
	}

	if spec := cluster.Spec.Service; spec == nil {
		service.Spec.Type = corev1.ServiceTypeClusterIP
	} else {
		service.Spec.Type = corev1.ServiceType(spec.Type)
		// K8SPG-389
		service.Spec.LoadBalancerSourceRanges = spec.LoadBalancerSourceRanges

		if spec.NodePort != nil {
			if service.Spec.Type == corev1.ServiceTypeClusterIP {
				// The NodePort can only be set when the Service type is NodePort or
				// LoadBalancer. However, due to a known issue prior to Kubernetes
				// 1.20, we clear these errors during our apply. To preserve the
				// appropriate behavior, we log an Event and return an error.
				// TODO(tjmoore4): Once Validation Rules are available, this check
				// and event could potentially be removed in favor of that validation
				recorder.Eventf(cluster, corev1.EventTypeWarning, "MisconfiguredClusterIP",
					"NodePort cannot be set with type ClusterIP on Service %q", service.Name)
				return nil, errors.Errorf("NodePort cannot be set with type ClusterIP on Service %q", service.Name)
			}
			servicePort.NodePort = *spec.NodePort
		}
		service.Spec.ExternalTrafficPolicy = initialize.FromPointer(spec.ExternalTrafficPolicy)
		service.Spec.InternalTrafficPolicy = spec.InternalTrafficPolicy
	}
	service.Spec.Ports = []corev1.ServicePort{servicePort}

	return service, nil
}

// primaryServiceViaLeader builds the primary Service as a headless Service
// whose Endpoints the operator points at leader's ClusterIP.
//
// We want to name and label our primary Service consistently, but the leader
// Service must be named after the Patroni "scope", which has its own
// constraints. Resolving through the leader's ClusterIP keeps the primary
// Service free of them.
func primaryServiceViaLeader(cluster *v1beta1.PostgresCluster, leader *corev1.Service) (
	corev1.ServiceSpec, *corev1.EndpointSubset, error,
) {
	if leader == nil {
		return corev1.ServiceSpec{}, nil, errors.New("Patroni leader Service is not available yet")
	}

	// Allocate no IP address (headless) and manage the Endpoints ourselves.
	// - https://docs.k8s.io/concepts/services-networking/service/#headless-services
	// - https://docs.k8s.io/concepts/services-networking/service/#services-without-selectors
	spec := corev1.ServiceSpec{
		ClusterIP: corev1.ClusterIPNone,
		Selector:  nil,
		Ports: []corev1.ServicePort{{
			Name:       naming.PortPostgreSQL,
			Port:       *cluster.Spec.Port,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromString(naming.PortPostgreSQL),
		}},
	}

	subset := &corev1.EndpointSubset{
		Addresses: []corev1.EndpointAddress{{IP: leader.Spec.ClusterIP}},
	}

	// Copy the EndpointPorts from the ServicePorts.
	for _, sp := range spec.Ports {
		subset.Ports = append(subset.Ports, corev1.EndpointPort{
			Name:     sp.Name,
			Port:     sp.Port,
			Protocol: sp.Protocol,
		})
	}

	return spec, subset, nil
}

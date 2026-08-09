// Copyright 2021 - 2024 Crunchy Data Solutions, Inc.
//
// SPDX-License-Identifier: Apache-2.0

package dcs

import (
	"context"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/percona/percona-postgresql-operator/v2/internal/logging"
	"github.com/percona/percona-postgresql-operator/v2/internal/naming"
	"github.com/percona/percona-postgresql-operator/v2/internal/patroni"
	"github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// kubernetesEndpointsBackend uses Kubernetes Endpoints as Patroni's DCS.
// This is distinct from a future ConfigMaps-based Kubernetes backend, which
// Patroni also supports.
type kubernetesEndpointsBackend struct{}

func (kubernetesEndpointsBackend) ClusterYAML(cluster *v1beta1.PostgresCluster) map[string]any {
	labels := map[string]string{naming.LabelCluster: cluster.Name}
	if cluster.CompareVersion("2.9.0") >= 0 {
		labels = naming.Merge(cluster.Spec.Metadata.GetLabelsOrNil(), labels)
	}

	return map[string]any{
		// Use Kubernetes Endpoints for the distributed configuration store (DCS).
		// These values cannot change during the cluster's lifetime.
		//
		// NOTE(cbandy): It *might* be possible to *carefully* change the role and
		// scope labels, but there is no way to reconfigure all instances at once.
		"kubernetes": map[string]any{
			"namespace":     cluster.Namespace,
			"role_label":    naming.LabelRole,
			"scope_label":   naming.LabelPatroni,
			"use_endpoints": true,

			// In addition to "scope_label" above, Patroni will add the following to
			// every object it creates. It will also use these as filters when doing
			// any lookups.
			"labels": labels,
		},
	}
}

func (kubernetesEndpointsBackend) InstanceYAML(*v1beta1.PostgresCluster) map[string]any {
	return nil
}

func (kubernetesEndpointsBackend) PodAdditions(
	_ *v1beta1.PostgresCluster, leaderService *corev1.Service, podContainers []corev1.Container,
) patroni.PodAdditions {
	// "kubernetes.pod_ip" and "kubernetes.ports" cannot be known until the
	// instance Pod is created, so they aren't set in InstanceYAML. Instead
	// they're injected using the downward API via the
	// PATRONI_KUBERNETES_POD_IP and PATRONI_KUBERNETES_PORTS env vars below.
	// Gather Endpoint ports for any Container ports that match the leader
	// Service definition.
	ports := []corev1.EndpointPort{}
	for _, sp := range leaderService.Spec.Ports {
		for i := range podContainers {
			for _, cp := range podContainers[i].Ports {
				if sp.TargetPort.StrVal == cp.Name {
					ports = append(ports, corev1.EndpointPort{
						Name:     sp.Name,
						Port:     cp.ContainerPort,
						Protocol: cp.Protocol,
					})
				}
			}
		}
	}
	portsYAML, _ := yaml.Marshal(ports)

	return patroni.PodAdditions{EnvVars: []corev1.EnvVar{
		// Set "kubernetes.pod_ip" to the v1.Pod's primary IP address.
		// Patroni must be restarted when changing this value.
		{
			Name: "PATRONI_KUBERNETES_POD_IP",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  "status.podIP",
			}},
		},

		// When using Endpoints for DCS, Patroni needs to replicate the leader
		// ServicePort definitions. Set "kubernetes.ports" to the YAML of this
		// Pod's equivalent EndpointPort definitions.
		//
		// This is connascent with PATRONI_POSTGRESQL_CONNECT_ADDRESS.
		// Patroni must be restarted when changing this value.
		{
			Name:  "PATRONI_KUBERNETES_PORTS",
			Value: string(portsYAML),
		},
	}}
}

// When using Endpoints for DCS, "create", "list", "patch", and "watch" are
// required. Include "get" for good measure. The `patronictl scaffold` and
// `patronictl remove` commands require "deletecollection".
// +kubebuilder:rbac:namespace=patroni,groups="",resources="endpoints",verbs={get}
// +kubebuilder:rbac:namespace=patroni,groups="",resources="endpoints",verbs={create,deletecollection}
// +kubebuilder:rbac:namespace=patroni,groups="",resources="endpoints",verbs={list,watch}
// +kubebuilder:rbac:namespace=patroni,groups="",resources="endpoints",verbs={patch}
// +kubebuilder:rbac:namespace=patroni,groups="",resources="services",verbs={create}

// The OpenShift RestrictedEndpointsAdmission plugin requires special
// authorization to create Endpoints that contain Pod IPs.
// - https://github.com/openshift/origin/pull/9383
// +kubebuilder:rbac:namespace=patroni,groups="",resources="endpoints/restricted",verbs={create}

func (kubernetesEndpointsBackend) Permissions(cluster *v1beta1.PostgresCluster) []rbacv1.PolicyRule {
	rules := make([]rbacv1.PolicyRule, 0, 3)

	rules = append(rules, rbacv1.PolicyRule{
		APIGroups: []string{corev1.SchemeGroupVersion.Group},
		Resources: []string{"endpoints"},
		Verbs:     []string{"create", "deletecollection", "get", "list", "patch", "watch"},
	})

	if cluster.Spec.OpenShift != nil && *cluster.Spec.OpenShift {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{corev1.SchemeGroupVersion.Group},
			Resources: []string{"endpoints/restricted"},
			Verbs:     []string{"create"},
		})
	}

	// When using Endpoints for DCS, Patroni tries to create the "{scope}-config" service.
	// NOTE(cbandy): The PostgresCluster controller already creates this Service;
	// it might be possible to eliminate this permission if it also created the
	// Endpoints.
	rules = append(rules, rbacv1.PolicyRule{
		APIGroups: []string{corev1.SchemeGroupVersion.Group},
		Resources: []string{"services"},
		Verbs:     []string{"create"},
	})

	return rules
}

func (kubernetesEndpointsBackend) DistributedConfigurationService(cluster *v1beta1.PostgresCluster) *corev1.Service {
	// When using Endpoints for DCS, Patroni needs a Service to ensure that the
	// Endpoints object is not removed by Kubernetes at startup. Patroni will
	// create this object if it has permission to do so, but it won't set any
	// ownership.
	// - https://releases.k8s.io/v1.16.0/pkg/controller/endpoint/endpoints_controller.go#L547
	// - https://releases.k8s.io/v1.20.0/pkg/controller/endpoint/endpoints_controller.go#L580
	// - https://github.com/zalando/patroni/blob/v2.0.1/patroni/dcs/kubernetes.py#L865-L881
	service := &corev1.Service{ObjectMeta: naming.PatroniDistributedConfiguration(cluster)}
	service.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))

	// Allocate no IP address (headless) and create no Endpoints.
	// - https://docs.k8s.io/concepts/services-networking/service/#headless-services
	service.Spec.ClusterIP = corev1.ClusterIPNone
	service.Spec.Selector = nil

	return service
}

func (kubernetesEndpointsBackend) LeaderService(
	cluster *v1beta1.PostgresCluster, recorder record.EventRecorder,
) (*corev1.Service, error) {
	// Allocate an IP address and/or node port and let Patroni manage the
	// Endpoints. Patroni will ensure that they always route to the elected
	// leader, so this Service needs no selector of its own.
	// - https://docs.k8s.io/concepts/services-networking/service/#services-without-selectors
	return leaderService(cluster, recorder, nil)
}

func (kubernetesEndpointsBackend) PrimaryService(
	cluster *v1beta1.PostgresCluster, leader *corev1.Service,
) (corev1.ServiceSpec, *corev1.EndpointSubset, error) {
	return primaryServiceViaLeader(cluster, leader)
}

func (kubernetesEndpointsBackend) Observe(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
	readyInstance bool, _ *corev1.Pod, _ patroni.Executor,
) (Observation, error) {
	var observation Observation

	dcs := &corev1.Endpoints{ObjectMeta: naming.PatroniDistributedConfiguration(cluster)}
	err := errors.WithStack(client.IgnoreNotFound(
		cli.Get(ctx, client.ObjectKeyFromObject(dcs), dcs),
	))

	if err == nil {
		if dcs.Annotations["initialize"] != "" {
			// After bootstrap, Patroni writes the cluster system identifier to DCS.
			observation.SystemIdentifier = dcs.Annotations["initialize"]
		} else if readyInstance {
			// While we typically expect a value for the initialize key to be present in the
			// Endpoints above by the time the StatefulSet for any instance indicates "ready"
			// (since Patroni writes this value after successful cluster bootstrap, at which time
			// the initial primary should transition to "ready"), sometimes this is not the case
			// and the "initialize" key is not yet present.  Therefore, if a "ready" instance
			// is detected in the cluster we assume this is the case, and simply log a message and
			// requeue in order to try again until the expected value is found.
			logging.FromContext(ctx).Info("detected ready instance but no initialize value")
			observation.RetryAfter = time.Second
		}
	}

	return observation, err
}

// PollInterval is zero: Patroni writes its state into Kubernetes objects, and
// the operator's watches turn those writes into reconciles.
func (kubernetesEndpointsBackend) PollInterval(*v1beta1.PostgresCluster) time.Duration {
	return 0
}

// PodRequiresRestart reads the "status" annotation Patroni writes on its own
// Pod when using Kubernetes for DCS.
func (kubernetesEndpointsBackend) PodRequiresRestart(
	_ context.Context, _ *v1beta1.PostgresCluster, pod *corev1.Pod, _ patroni.Executor,
) (bool, error) {
	return patroni.PodRequiresRestart(pod), nil
}

// ClearState reports on the Endpoints objects Patroni uses to hold the leader
// lock, the distributed configuration, and any pending failover. Deleting them
// makes the cluster forget it was ever initialized.
func (kubernetesEndpointsBackend) ClearState(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster, _ string,
) (StateCleanup, error) {
	var cleanup StateCleanup

	for _, meta := range []metav1.ObjectMeta{
		naming.PatroniLeaderEndpoints(cluster),
		naming.PatroniDistributedConfiguration(cluster),
		naming.PatroniTrigger(cluster),
	} {
		endpoints := &corev1.Endpoints{ObjectMeta: meta}
		err := cli.Get(ctx, client.ObjectKeyFromObject(endpoints), endpoints)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return cleanup, errors.WithStack(err)
		}
		cleanup.Delete = append(cleanup.Delete, endpoints)
	}

	cleanup.Cleared = len(cleanup.Delete) == 0
	return cleanup, nil
}

func (kubernetesEndpointsBackend) Delete(
	ctx context.Context, cli client.Client, cluster *v1beta1.PostgresCluster,
) (StateCleanup, error) {
	// TODO(cbandy): This could also be accomplished by adopting the Endpoints
	// as Patroni creates them. Would their events cause too many reconciles?
	// Foreground deletion may force us to adopt and set finalizers anyway.
	selector, err := naming.AsSelector(naming.ClusterPatronis(cluster))
	if err == nil {
		err = errors.WithStack(
			cli.DeleteAllOf(
				ctx, &corev1.Endpoints{},
				client.InNamespace(cluster.Namespace),
				client.MatchingLabelsSelector{Selector: selector},
			),
		)
	}

	// DeleteAllOf is synchronous, so there is nothing to wait for.
	return StateCleanup{Cleared: true}, err
}

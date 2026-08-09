//go:build envtest

package pgcluster

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
)

// These exercise the CEL rules on the CRD against a real API server.
//
// The immutability rule is written against the whole spec rather than against
// spec.patroni.dcs, because a transition rule only runs when its field exists
// in both the old and new object -- and a cluster created without a "dcs"
// section is the common case. A rule on spec.patroni.dcs would let such a
// cluster be switched to etcd simply by adding the section.
var _ = Describe("Patroni DCS validation", Ordered, func() {
	ctx := context.Background()

	const ns = "dcs-validation"
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
	})
	AfterAll(func() {
		_ = k8sClient.Delete(ctx, namespace)
	})

	etcdDCS := func() *crunchyv1beta1.PatroniDCS {
		return &crunchyv1beta1.PatroniDCS{
			Type: crunchyv1beta1.PatroniDCSTypeEtcd,
			Etcd: &crunchyv1beta1.PatroniEtcdSpec{
				Endpoints: []string{"http://etcd.svc:2379"},
			},
		}
	}

	newCluster := func(name string, dcs *crunchyv1beta1.PatroniDCS) *v2.PerconaPGCluster {
		cr, err := readDefaultCR(name, ns)
		Expect(err).NotTo(HaveOccurred())

		if dcs != nil {
			if cr.Spec.Patroni == nil {
				cr.Spec.Patroni = new(crunchyv1beta1.PatroniSpec)
			}
			cr.Spec.Patroni.DCS = dcs
		}
		return cr
	}

	It("rejects switching an existing cluster to etcd by adding the dcs section", func() {
		cr := newCluster("dcs-immutable-add", nil)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)).To(Succeed())
		if cr.Spec.Patroni == nil {
			cr.Spec.Patroni = new(crunchyv1beta1.PatroniSpec)
		}
		cr.Spec.Patroni.DCS = etcdDCS()

		err := k8sClient.Update(ctx, cr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("immutable after cluster creation"))
	})

	It("allows adding an explicitly-kubernetes dcs section", func() {
		cr := newCluster("dcs-immutable-same", nil)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)).To(Succeed())
		if cr.Spec.Patroni == nil {
			cr.Spec.Patroni = new(crunchyv1beta1.PatroniSpec)
		}
		cr.Spec.Patroni.DCS = &crunchyv1beta1.PatroniDCS{
			Type: crunchyv1beta1.PatroniDCSTypeKubernetes,
		}

		// The effective type did not change, so this is not a DCS switch.
		Expect(k8sClient.Update(ctx, cr)).To(Succeed())
	})

	It("rejects switching an etcd cluster back to kubernetes", func() {
		cr := newCluster("dcs-immutable-remove", etcdDCS())
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)).To(Succeed())
		cr.Spec.Patroni.DCS.Type = crunchyv1beta1.PatroniDCSTypeKubernetes

		err := k8sClient.Update(ctx, cr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("immutable after cluster creation"))
	})

	It("requires endpoints when the type is etcd", func() {
		cr := newCluster("dcs-etcd-no-endpoints", &crunchyv1beta1.PatroniDCS{
			Type: crunchyv1beta1.PatroniDCSTypeEtcd,
		})

		err := k8sClient.Create(ctx, cr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("etcd.endpoints must be non-empty"))
	})

	It("requires every endpoint to use the same scheme", func() {
		cr := newCluster("dcs-etcd-mixed-schemes", &crunchyv1beta1.PatroniDCS{
			Type: crunchyv1beta1.PatroniDCSTypeEtcd,
			Etcd: &crunchyv1beta1.PatroniEtcdSpec{
				Endpoints: []string{"http://a.svc:2379", "https://b.svc:2379"},
			},
		})

		err := k8sClient.Create(ctx, cr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("same scheme"))
	})
})

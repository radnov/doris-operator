// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	dorisv1 "github.com/apache/doris-operator/api/doris/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// The FE pod turns ready before the endpoints of the FE service list its address, so the BE is skipped
// on that reconcile and the later endpoints update is the only change left. The BE must still be
// created. The endpoints controller copies the service labels onto the endpoints, which the watch on
// the FE endpoints relies on. Without those labels only the requeue of a cluster that is not available
// yet can create the BE.
func TestBECreatedWhenFEEndpointsBecomeReadyAfterFEPod(t *testing.T) {
	environment := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{dorisv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	direct, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	(&DorisClusterReconciler{}).Init(manager, &Options{})
	stopped := make(chan error, 1)
	go func() { stopped <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})

	for _, tc := range []struct {
		name            string
		labelEndpoints  bool
		beCreatedWithin time.Duration
	}{
		{name: "watch on labelled FE endpoints", labelEndpoints: true, beCreatedWithin: time.Second},
		{name: "requeue while BE is not available", labelEndpoints: false, beCreatedWithin: 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "fe-endpoints-"}}
			if err := direct.Create(ctx, namespace); err != nil {
				t.Fatal(err)
			}
			cluster := &dorisv1.DorisCluster{ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: namespace.Name}, Spec: dorisv1.DorisClusterSpec{
				FeSpec: &dorisv1.FeSpec{BaseSpec: dorisv1.BaseSpec{Replicas: ptr.To(int32(1)), Image: "doris:fe"}},
				BeSpec: &dorisv1.BeSpec{BaseSpec: dorisv1.BaseSpec{Replicas: ptr.To(int32(1)), Image: "doris:be"}},
			}}
			if err := direct.Create(ctx, cluster); err != nil {
				t.Fatal(err)
			}

			feStatefulSet := &appsv1.StatefulSet{}
			eventually(t, 30*time.Second, "FE StatefulSet created", func() bool {
				return direct.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: dorisv1.GenerateComponentStatefulSetName(cluster, dorisv1.Component_FE)}, feStatefulSet) == nil
			})
			feService := &corev1.Service{}
			feServiceName := dorisv1.GenerateExternalServiceName(cluster, dorisv1.Component_FE)
			eventually(t, 10*time.Second, "FE service created", func() bool {
				return direct.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: feServiceName}, feService) == nil
			})
			endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: feServiceName, Namespace: namespace.Name}}
			if tc.labelEndpoints {
				endpoints.Labels = feService.Labels
			}
			if err := direct.Create(ctx, endpoints); err != nil {
				t.Fatal(err)
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: feStatefulSet.Name + "-0", Namespace: namespace.Name, Labels: feStatefulSet.Spec.Template.Labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "fe", Image: "doris:fe"}}},
			}
			if err := direct.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.0.0.10"
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "fe", Image: "doris:fe", Ready: true}}
			if err := direct.Status().Update(ctx, pod); err != nil {
				t.Fatal(err)
			}

			eventually(t, 10*time.Second, "FE status available", func() bool {
				current := &dorisv1.DorisCluster{}
				if err := direct.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
					return false
				}
				return current.Status.FEStatus != nil && current.Status.FEStatus.ComponentCondition.Phase == dorisv1.Available
			})
			// Let the reconciles caused by the pod and status updates finish before the endpoints change.
			time.Sleep(3 * time.Second)
			beStatefulSetName := dorisv1.GenerateComponentStatefulSetName(cluster, dorisv1.Component_BE)
			if err := direct.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: beStatefulSetName}, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
				t.Fatalf("BE StatefulSet must not exist while the FE endpoints have no address, got %v", err)
			}

			endpoints.Subsets = []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{IP: pod.Status.PodIP}},
				Ports:     []corev1.EndpointPort{{Name: "query-port", Port: 9030}},
			}}
			if err := direct.Update(ctx, endpoints); err != nil {
				t.Fatal(err)
			}

			eventually(t, tc.beCreatedWithin, "BE StatefulSet created after the FE endpoints became ready", func() bool {
				return direct.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: beStatefulSetName}, &appsv1.StatefulSet{}) == nil
			})
		})
	}
}

func TestFEEndpointsToDorisCluster(t *testing.T) {
	cluster := &dorisv1.DorisCluster{ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "doris-system"}}
	feAccessLabels := dorisv1.GenerateExternalServiceLabels(cluster, dorisv1.Component_FE)
	beAccessLabels := dorisv1.GenerateExternalServiceLabels(cluster, dorisv1.Component_BE)
	feInternalLabels := dorisv1.GenerateExternalServiceLabels(cluster, dorisv1.Component_FE)
	feInternalLabels[dorisv1.ServiceRoleForCluster] = string(dorisv1.Service_Role_Internal)
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   []string
	}{
		{name: "FE access service endpoints", labels: feAccessLabels, want: []string{"doris-system/doris"}},
		{name: "BE access service endpoints", labels: beAccessLabels},
		{name: "FE internal service endpoints", labels: feInternalLabels},
		{name: "unrelated endpoints", labels: map[string]string{"app": "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: "endpoints", Namespace: "doris-system", Labels: tc.labels}}
			var got []string
			for _, request := range feEndpointsToDorisCluster(context.Background(), endpoints) {
				got = append(got, request.NamespacedName.String())
			}
			if len(got) != len(tc.want) || (len(got) == 1 && got[0] != tc.want[0]) {
				t.Fatalf("requests=%v, want %v", got, tc.want)
			}
		})
	}
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, description)
}

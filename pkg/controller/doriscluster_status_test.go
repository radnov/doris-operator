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
	"testing"
	"time"

	dorisv1 "github.com/apache/doris-operator/api/doris/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKeepTransitionTimes(t *testing.T) {
	storedTime := metav1.NewTime(time.Date(2026, 11, 4, 16, 16, 59, 0, time.UTC))
	newTime := metav1.NewTime(time.Date(2026, 11, 4, 16, 17, 30, 123, time.UTC))
	condition := func(phase dorisv1.ComponentPhase, at metav1.Time) dorisv1.ComponentCondition {
		return dorisv1.ComponentCondition{Phase: phase, LastTransitionTime: at}
	}
	stored := dorisv1.DorisClusterStatus{
		FEStatus:     &dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Available, storedTime)},
		BEStatus:     &dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Initializing, storedTime)},
		CnStatus:     &dorisv1.CnStatus{ComponentStatus: dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Initializing, storedTime)}},
		BrokerStatus: &dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Initializing, storedTime)},
	}
	status := dorisv1.DorisClusterStatus{
		FEStatus:     &dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Available, newTime)},
		BEStatus:     &dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Available, newTime)},
		CnStatus:     &dorisv1.CnStatus{ComponentStatus: dorisv1.ComponentStatus{ComponentCondition: condition(dorisv1.Initializing, newTime)}},
		BrokerStatus: nil,
	}

	keepTransitionTimes(&status, &stored)

	for _, check := range []struct {
		component string
		got       metav1.Time
		want      metav1.Time
	}{
		{"FE kept its phase", status.FEStatus.ComponentCondition.LastTransitionTime, storedTime},
		{"BE changed its phase", status.BEStatus.ComponentCondition.LastTransitionTime, newTime},
		{"CN kept its phase", status.CnStatus.ComponentCondition.LastTransitionTime, storedTime},
	} {
		if !check.got.Equal(&check.want) {
			t.Errorf("%s: LastTransitionTime=%v, want %v", check.component, check.got, check.want)
		}
	}
}

func TestUpdateDorisClusterStatus(t *testing.T) {
	storedTime := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	for _, tc := range []struct {
		name          string
		storedFEPhase dorisv1.ComponentPhase
		newBEPhase    dorisv1.ComponentPhase
		wantRequeue   bool
		wantWrite     bool
	}{
		// Each reconcile builds the conditions with the current time. When no phase changed, the status
		// must still compare as unchanged, so the cluster is requeued while the BE is not available.
		{name: "unchanged phases and BE not available", storedFEPhase: dorisv1.Available, newBEPhase: dorisv1.Initializing, wantRequeue: true, wantWrite: false},
		// A changed phase is written, and the write itself enqueues the next reconcile.
		{name: "FE became available", storedFEPhase: dorisv1.Initializing, newBEPhase: dorisv1.Initializing, wantRequeue: false, wantWrite: true},
		{name: "all components available", storedFEPhase: dorisv1.Available, newBEPhase: dorisv1.Available, wantRequeue: false, wantWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := dorisv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			stored := &dorisv1.DorisCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "doris-system"},
				Spec: dorisv1.DorisClusterSpec{
					FeSpec: &dorisv1.FeSpec{BaseSpec: dorisv1.BaseSpec{Replicas: ptr.To(int32(1)), Image: "doris:fe"}},
					BeSpec: &dorisv1.BeSpec{BaseSpec: dorisv1.BaseSpec{Replicas: ptr.To(int32(1)), Image: "doris:be"}},
				},
				Status: dorisv1.DorisClusterStatus{
					FEStatus: &dorisv1.ComponentStatus{ComponentCondition: dorisv1.ComponentCondition{Phase: tc.storedFEPhase, LastTransitionTime: storedTime}},
					BEStatus: &dorisv1.ComponentStatus{ComponentCondition: dorisv1.ComponentCondition{Phase: dorisv1.Initializing, LastTransitionTime: storedTime}},
				},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stored).WithObjects(stored).Build()
			r := &DorisClusterReconciler{Client: c}
			before := &dorisv1.DorisCluster{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(stored), before); err != nil {
				t.Fatal(err)
			}

			reconciled := before.DeepCopy()
			now := metav1.Now()
			reconciled.Status.FEStatus.ComponentCondition = dorisv1.ComponentCondition{Phase: dorisv1.Available, LastTransitionTime: now}
			reconciled.Status.BEStatus.ComponentCondition = dorisv1.ComponentCondition{Phase: tc.newBEPhase, LastTransitionTime: now}

			result, err := r.updateDorisClusterStatus(context.Background(), reconciled)
			if err != nil {
				t.Fatal(err)
			}
			if gotRequeue := result.RequeueAfter > 0; gotRequeue != tc.wantRequeue {
				t.Fatalf("requeue=%v (RequeueAfter=%v), want %v", gotRequeue, result.RequeueAfter, tc.wantRequeue)
			}

			after := &dorisv1.DorisCluster{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(stored), after); err != nil {
				t.Fatal(err)
			}
			if gotWrite := after.ResourceVersion != before.ResourceVersion; gotWrite != tc.wantWrite {
				t.Fatalf("status written=%v, want %v", gotWrite, tc.wantWrite)
			}
			if after.Status.FEStatus.ComponentCondition.Phase != dorisv1.Available {
				t.Fatalf("stored FE phase=%s, want %s", after.Status.FEStatus.ComponentCondition.Phase, dorisv1.Available)
			}
			if tc.storedFEPhase == dorisv1.Available && !after.Status.FEStatus.ComponentCondition.LastTransitionTime.Equal(&storedTime) {
				t.Fatalf("FE LastTransitionTime=%v, want the stored %v because the phase did not change", after.Status.FEStatus.ComponentCondition.LastTransitionTime, storedTime)
			}
		})
	}
}

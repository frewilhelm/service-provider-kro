/*
Copyright 2025.

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

package controller

import (
	"context"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

func TestObserveFluxResourcePhase(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sourcev1.AddToScheme(scheme)
	_ = helmv2.AddToScheme(scheme)

	tests := []struct {
		name      string
		obj       client.Object
		wantPhase apiv1alpha1.InstancePhase
		wantMsg   string
		wantErr   bool
	}{
		{
			name:      "resource not found",
			obj:       nil,
			wantPhase: apiv1alpha1.Pending,
			wantMsg:   "Resource not yet created",
		},
		{
			name: "no conditions",
			obj: &sourcev1.OCIRepository{
				ObjectMeta: metav1.ObjectMeta{
					Name:      OCIRepositoryName,
					Namespace: "test-ns",
				},
			},
			wantPhase: apiv1alpha1.Progressing,
			wantMsg:   "Waiting for first reconciliation",
		},
		{
			name: "ready condition true",
			obj: &sourcev1.OCIRepository{
				ObjectMeta: metav1.ObjectMeta{
					Name:      OCIRepositoryName,
					Namespace: "test-ns",
				},
				Status: sourcev1.OCIRepositoryStatus{
					Conditions: []metav1.Condition{
						{
							Type:    meta.ReadyCondition,
							Status:  metav1.ConditionTrue,
							Message: "stored artifact for revision abc123",
						},
					},
				},
			},
			wantPhase: apiv1alpha1.Ready,
			wantMsg:   "stored artifact for revision abc123",
		},
		{
			name: "ready condition false",
			obj: &helmv2.HelmRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      HelmReleaseName,
					Namespace: "test-ns",
				},
				Status: helmv2.HelmReleaseStatus{
					Conditions: []metav1.Condition{
						{
							Type:    meta.ReadyCondition,
							Status:  metav1.ConditionFalse,
							Message: "install retries exhausted",
						},
					},
				},
			},
			wantPhase: apiv1alpha1.Progressing,
			wantMsg:   "install retries exhausted",
		},
		{
			name: "stalled condition true",
			obj: &helmv2.HelmRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      HelmReleaseName,
					Namespace: "test-ns",
				},
				Status: helmv2.HelmReleaseStatus{
					Conditions: []metav1.Condition{
						{
							Type:    meta.ReadyCondition,
							Status:  metav1.ConditionFalse,
							Message: "install failed",
						},
						{
							Type:    meta.StalledCondition,
							Status:  metav1.ConditionTrue,
							Message: "reconciliation stalled: dependency not ready",
						},
					},
				},
			},
			wantPhase: apiv1alpha1.Failed,
			wantMsg:   "reconciliation stalled: dependency not ready",
		},
		{
			name: "stalled condition false does not override ready",
			obj: &sourcev1.OCIRepository{
				ObjectMeta: metav1.ObjectMeta{
					Name:      OCIRepositoryName,
					Namespace: "test-ns",
				},
				Status: sourcev1.OCIRepositoryStatus{
					Conditions: []metav1.Condition{
						{
							Type:    meta.ReadyCondition,
							Status:  metav1.ConditionTrue,
							Message: "artifact ready",
						},
						{
							Type:   meta.StalledCondition,
							Status: metav1.ConditionFalse,
						},
					},
				},
			},
			wantPhase: apiv1alpha1.Ready,
			wantMsg:   "artifact ready",
		},
		{
			name: "ready condition unknown",
			obj: &sourcev1.OCIRepository{
				ObjectMeta: metav1.ObjectMeta{
					Name:      OCIRepositoryName,
					Namespace: "test-ns",
				},
				Status: sourcev1.OCIRepositoryStatus{
					Conditions: []metav1.Condition{
						{
							Type:    meta.ReadyCondition,
							Status:  metav1.ConditionUnknown,
							Message: "reconciliation in progress",
						},
					},
				},
			},
			wantPhase: apiv1alpha1.Unknown,
			wantMsg:   "reconciliation in progress",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.obj != nil {
				builder = builder.WithObjects(tc.obj)
			}
			c := builder.Build()

			var queryObj client.Object
			key := client.ObjectKey{Namespace: "test-ns"}
			if tc.obj != nil {
				key.Name = tc.obj.GetName()
				switch tc.obj.(type) {
				case *sourcev1.OCIRepository:
					queryObj = &sourcev1.OCIRepository{}
				case *helmv2.HelmRelease:
					queryObj = &helmv2.HelmRelease{}
				}
			} else {
				key.Name = OCIRepositoryName
				queryObj = &sourcev1.OCIRepository{}
			}

			phase, msg, err := observeFluxResourcePhase(context.Background(), c, key, queryObj)

			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if phase != tc.wantPhase {
				t.Errorf("phase: got %q, want %q", phase, tc.wantPhase)
			}
			if msg != tc.wantMsg {
				t.Errorf("message: got %q, want %q", msg, tc.wantMsg)
			}
		})
	}
}

func TestAllReady(t *testing.T) {
	tests := []struct {
		name      string
		resources []apiv1alpha1.ManagedResource
		want      bool
	}{
		{
			name:      "empty slice",
			resources: nil,
			want:      true,
		},
		{
			name: "all ready",
			resources: []apiv1alpha1.ManagedResource{
				{Phase: apiv1alpha1.Ready},
				{Phase: apiv1alpha1.Ready},
			},
			want: true,
		},
		{
			name: "one progressing",
			resources: []apiv1alpha1.ManagedResource{
				{Phase: apiv1alpha1.Ready},
				{Phase: apiv1alpha1.Progressing},
			},
			want: false,
		},
		{
			name: "one failed",
			resources: []apiv1alpha1.ManagedResource{
				{Phase: apiv1alpha1.Failed},
				{Phase: apiv1alpha1.Ready},
			},
			want: false,
		},
		{
			name: "one pending",
			resources: []apiv1alpha1.ManagedResource{
				{Phase: apiv1alpha1.Pending},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := allReady(tc.resources)
			if got != tc.want {
				t.Errorf("allReady: got %v, want %v", got, tc.want)
			}
		})
	}
}

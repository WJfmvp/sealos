// Copyright © 2023 sealos.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	//"github.com/go-redis/redismock/v8"
	"github.com/labring/sealos/controllers/pkg/gpu"
	"github.com/labring/sealos/controllers/pkg/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

func milli(q *quantity) int64 {
	if q == nil {
		return 0
	}
	return q.MilliValue()
}

func TestCalculatePodResource(t *testing.T) {
	now := metav1.Now()
	reconciler := &MonitorReconciler{} // 这里只需要方法，不依赖其它字段

	tests := []struct {
		name     string
		pod      *corev1.Pod
		wantCPU  int64 // mCore
		wantMem  int64 // bytes
		wantGPUs int64 // count
	}{
		{
			name: "running pod, limits 优先生效",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-a",
					Namespace: "ns-a",
				},
				Spec: corev1.PodSpec{
					NodeName: "worker-1",
					Containers: []corev1.Container{
						{
							Name: "c1",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
						},
					},
				},
				Status: corev1.PodStatus{
					Phase:      corev1.PodRunning,
					StartTime:  &now,
					Conditions: nil,
				},
			},
			wantCPU:  500,               // 500m
			wantMem:  512 * 1024 * 1024, // 512 Mi
			wantGPUs: 0,
		},
		{
			name: "pending pod 超时 >1m 被跳过",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-pending",
					Namespace: "ns-p",
				},
				Spec: corev1.PodSpec{
					NodeName: "worker-2",
					Containers: []corev1.Container{
						{ // 即使声明 requests，也应被算作 0
							Name: "c1",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
							},
						},
					},
				},
				Status: corev1.PodStatus{
					Phase:     corev1.PodPending,
					StartTime: &metav1.Time{Time: time.Now().Add(-2 * time.Minute)},
				},
			},
			wantCPU:  0,
			wantMem:  0,
			wantGPUs: 0,
		},
	}

	for _, tt := range tests {
		got := reconciler.calculatePodResource(tt.pod)

		assert.Equal(t, tt.wantCPU, got[corev1.ResourceCPU].MilliValue(), tt.name+" cpu")
		assert.Equal(t, tt.wantMem, got[corev1.ResourceMemory].Value(), tt.name+" mem")
		assert.Equal(t, tt.wantGPUs, milli(got[gpu.NvidiaGpuKey]), tt.name+" gpu")
	}
}

func injectSnapshot(rdb *redis.Client, key string, curr int64) error {
	return rdb.HMSet(context.Background(), key, map[string]interface{}{
		"previous":  curr,
		"current":   curr,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}).Err()
}

func TestTrafficCache_Delta(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cache := &TrafficCache{Redis: rdb, TTL: time.Hour}

	ctx := context.Background()
	user, bucket := "alice", "bucket-1"
	key := "traffic:object:" + user + ":" + bucket

	// 1st sample —— cold start
	assert.Equal(t, int64(0), cache.Delta(ctx, user, bucket, 123))

	// 模拟 save 快照（current = 123）
	assert.NoError(t, injectSnapshot(rdb, key, 123))

	// 2nd sample —— 增长到 223，delta=100
	assert.Equal(t, int64(100), cache.Delta(ctx, user, bucket, 223))

	// 更新快照（current = 223）
	assert.NoError(t, injectSnapshot(rdb, key, 223))

	// 3rd sample —— 计数回绕 / 递减，被视为 0
	assert.Equal(t, int64(0), cache.Delta(ctx, user, bucket, 50))
}

func TestCalculatePodResource_WithGPU(t *testing.T) {
	now := metav1.Now()
	r := &MonitorReconciler{}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gpu-pod",
			Namespace: "ns-gpu",
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "c1",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),   // 1000m
							corev1.ResourceMemory: resource.MustParse("1Gi"), // 1 Gi
							gpu.NvidiaGpuKey:      resource.MustParse("2"),   // 2 GPU
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: &now,
		},
	}

	got := r.calculatePodResource(pod)

	assert.Equal(t, int64(1000), got[corev1.ResourceCPU].MilliValue())
	assert.Equal(t, int64(1<<30), got[corev1.ResourceMemory].Value())
	assert.Equal(t, int64(2), got[gpu.NvidiaGpuKey].Value())
}

func TestCalculatePodResource_PendingWarmupIncluded(t *testing.T) {
	start := metav1.NewTime(time.Now().Add(-30 * time.Second))
	r := &MonitorReconciler{}

	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "ns"},
		Spec: corev1.PodSpec{
			NodeName: "n",
			Containers: []corev1.Container{
				{
					Name: "c1",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("250m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodPending,
			StartTime: &start,
		},
	}

	res := r.calculatePodResource(p)
	assert.Equal(t, int64(250), res[corev1.ResourceCPU].MilliValue())
	assert.Equal(t, int64(128*1024*1024), res[corev1.ResourceMemory].Value())
	assert.Equal(t, int64(0), milli(res[gpu.NvidiaGpuKey]))
}

func TestGetResourceUsed_UnitCeilAndZeroFilter(t *testing.T) {
	r := &MonitorReconciler{
		Properties: &resources.PropertyTypeLS{
			StringMap: map[string]resources.PropertyType{
				corev1.ResourceCPU.String(): {
					Enum: 1,
					Unit: *resource.NewMilliQuantity(1000, resource.DecimalSI), // 1 Core
				},
				corev1.ResourceMemory.String(): {
					Enum: 2,
					Unit: *resource.NewQuantity(1<<20, resource.BinarySI), // 1 MiB
				},
			},
		},
	}

	m := initResources()
	m[corev1.ResourceCPU].Add(*resource.NewMilliQuantity(2300, resource.DecimalSI))       // 2.3 C
	m[corev1.ResourceMemory].Add(*resource.NewQuantity(512*1024*1024, resource.BinarySI)) // 512 Mi

	empty, used := r.getResourceUsed(m)
	assert.False(t, empty)
	assert.Equal(t, int64(3), used[1])   // ceil(2.3) = 3
	assert.Equal(t, int64(512), used[2]) // 512 Mi / 1 Mi
}

func TestMonitorPVCResourceUsage(t *testing.T) {
	ns := "ns-pvc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	sch := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(sch))

	// 🔑 注册 status.phase 索引
	builder := fake.NewClientBuilder().WithScheme(sch).WithObjects(pvc)
	builder.WithIndex(&corev1.PersistentVolumeClaim{}, "status.phase",
		func(obj ctrlclient.Object) []string {
			pvc := obj.(*corev1.PersistentVolumeClaim)
			return []string{string(pvc.Status.Phase)}
		})
	cl := builder.Build()

	r := &MonitorReconciler{Client: cl}

	resUsed := map[string]map[corev1.ResourceName]*quantity{}
	resNamed := map[string]*resources.ResourceNamed{}

	err := r.monitorPVCResourceUsage(ns, resUsed, resNamed, nil)
	require.NoError(t, err)

	found := false
	for _, m := range resUsed {
		assert.Equal(t, int64(10<<30), m[corev1.ResourceStorage].Value())
		found = true
	}
	assert.True(t, found, "PVC usage should exist")
}

func TestMonitorServiceResourceUsage_NodePorts(t *testing.T) {
	ns := "ns-svc"
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "np", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{
				{Port: 80, NodePort: 30080},
				{Port: 81, NodePort: 30081},
			},
		},
	}

	sch := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(sch))

	// 关键：为 Service 注册 "spec.type" 字段索引（与生产代码保持同名）
	builder := fake.NewClientBuilder().WithScheme(sch).WithObjects(svc)
	builder.WithIndex(&corev1.Service{}, "spec.type",
		func(obj ctrlclient.Object) []string {
			s := obj.(*corev1.Service)
			return []string{string(s.Spec.Type)}
		})
	cl := builder.Build()

	r := &MonitorReconciler{Client: cl}
	resUsed := map[string]map[corev1.ResourceName]*quantity{}
	resNamed := map[string]*resources.ResourceNamed{}

	require.NoError(t, r.monitorServiceResourceUsage(ns, resUsed, resNamed, nil))

	found := false
	for _, m := range resUsed {
		assert.Equal(t, int64(2000), m[corev1.ResourceServicesNodePorts].Value()) // 2×1000
		found = true
	}
	assert.True(t, found)
}

func TestTrafficCache_SaveTrafficAndDelta_WithPipeline(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cache := &TrafficCache{Redis: rdb, TTL: time.Hour}

	ctx := context.Background()
	user, bucket := "bob", "bucket-x"
	key := fmt.Sprintf("traffic:object:%s:%s", user, bucket)

	// --------- 模拟第一次 SaveTraffic(current=456) 的结果 ---------
	// SaveTraffic 的语义：previous = current(旧值或首次等于 456)、current = 456、timestamp = now
	err = rdb.HMSet(ctx, key, map[string]interface{}{
		"previous":  int64(456),
		"current":   int64(456),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}).Err()
	require.NoError(t, err)
	require.NoError(t, rdb.Expire(ctx, key, cache.TTL).Err())

	// 验证写入
	hash, err := rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "456", hash["current"])
	require.Equal(t, "456", hash["previous"])
	_, ok := hash["timestamp"]
	require.True(t, ok, "timestamp should exist")

	// --------- Delta：556 - 456 = 100 ---------
	require.Equal(t, int64(100), cache.Delta(ctx, user, bucket, 556))

	// --------- 模拟第二次 SaveTraffic(current=556) 的结果 ---------
	err = rdb.HMSet(ctx, key, map[string]interface{}{
		"previous":  int64(556),
		"current":   int64(556),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}).Err()
	require.NoError(t, err)
	require.NoError(t, rdb.Expire(ctx, key, cache.TTL).Err())

	// 回绕（100 < 556）→ 0
	require.Equal(t, int64(0), cache.Delta(ctx, user, bucket, 100))
}

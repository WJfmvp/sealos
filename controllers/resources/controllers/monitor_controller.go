/*
Copyright 2023 sealos.

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

package controllers

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"k8s.io/apimachinery/pkg/api/errors"
	"math"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "github.com/labring/sealos/controllers/account/api/v1"

	"github.com/labring/sealos/controllers/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/labring/sealos/controllers/app/api/v1"

	kbv1alpha1 "github.com/apecloud/kubeblocks/apis/dataprotection/v1alpha1"

	"golang.org/x/sync/errgroup"

	"golang.org/x/sync/semaphore"

	"k8s.io/apimachinery/pkg/selection"

	"k8s.io/apimachinery/pkg/labels"

	userv1 "github.com/labring/sealos/controllers/user/api/v1"

	"github.com/labring/sealos/controllers/user/controllers/helper/config"

	"github.com/minio/minio-go/v7"

	objstorage "github.com/labring/sealos/controllers/pkg/objectstorage"

	"github.com/go-logr/logr"

	"github.com/labring/sealos/controllers/pkg/database"
	"github.com/labring/sealos/controllers/pkg/gpu"
	"github.com/labring/sealos/controllers/pkg/resources"
	"github.com/labring/sealos/controllers/pkg/utils/logger"
	"github.com/labring/sealos/controllers/pkg/utils/retry"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MonitorReconciler reconciles a Monitor object
type MonitorReconciler struct {
	client.Client
	logr.Logger
	Interval          time.Duration
	Scheme            *runtime.Scheme
	stopCh            chan struct{}
	wg                sync.WaitGroup
	periodicReconcile time.Duration

	// GPU 缓存相关
	NvidiaGpu map[string]gpu.NvidiaGPU
	gpuMutex  sync.Mutex

	// 依赖客户端
	DBClient                database.Interface
	TrafficClient           database.Interface
	ObjStorageClient        *minio.Client
	ObjStorageMetricsClient *objstorage.MetricsClient

	// 属性、配置
	Properties            *resources.PropertyTypeLS
	PromURL               string
	ObjectStorageInstance string

	lastObjectMetrics        objstorage.Metrics
	currentObjectMetrics     objstorage.Metrics
	ObjStorageUserBackupSize map[string]int64

	RedisClient    *redis.Client
	TrafficCache   *TrafficCache
	mu             sync.Mutex
	podResUsageMap map[string]map[string]map[corev1.ResourceName]*quantity
}

type TrafficCache struct {
	Redis *redis.Client
	TTL   time.Duration
}
type quantity struct {
	*resource.Quantity
	detail string
}

const (
	PrometheusURL         = "PROM_URL"
	ObjectStorageInstance = "OBJECT_STORAGE_INSTANCE"
	ConcurrentLimit       = "CONCURRENT_LIMIT"
)

var concurrentLimit = int64(DefaultConcurrencyLimit)

const (
	DefaultConcurrencyLimit = 1000
)

type NodeReconciler struct {
	client.Client
	logr.Logger
	parent *MonitorReconciler // 用于刷新 GPU 缓存
}

const (
	trafficCollectDelay = 10 * time.Second // 延迟时间
	trafficWindowLead   = 10 * time.Second // 滑窗前移时间
	trafficWindowLag    = 70 * time.Second // 滑窗后移时间
	flushInterval       = 3 * time.Minute  // 批量写入间隔
)

//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=resourcequotas,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=resourcequotas/status,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=services/status,verbs=get;list;watch
//+kubebuilder:rbac:groups=app.sealos.io,resources=instances,verbs=get;list;watch
//+kubebuilder:rbac:groups=app.sealos.io,resources=instances/status,verbs=get;list;watch
//+kubebuilder:rbac:groups=dataprotection.apecloud.io,resources=backups,verbs=get;list;watch
//+kubebuilder:rbac:groups=dataprotection.apecloud.io,resources=backups/status,verbs=get;list;watch

func NewMonitorReconciler(mgr ctrl.Manager) (*MonitorReconciler, error) {
	r := &MonitorReconciler{
		Client:                mgr.GetClient(),
		Logger:                ctrl.Log.WithName("controllers").WithName("Monitor"),
		stopCh:                make(chan struct{}),
		periodicReconcile:     1 * time.Minute,
		PromURL:               os.Getenv(PrometheusURL),
		ObjectStorageInstance: os.Getenv(ObjectStorageInstance),
		NvidiaGpu:             make(map[string]gpu.NvidiaGPU),
		podResUsageMap:        make(map[string]map[string]map[corev1.ResourceName]*quantity),
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379" // fallback
	}
	r.RedisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})
	r.TrafficCache = &TrafficCache{Redis: r.RedisClient, TTL: 2 * time.Hour}

	// ② 初始化 GPU 缓存
	if err := retry.Retry(2, time.Second, func() error {
		var err error
		r.NvidiaGpu, err = gpu.GetNodeGpuModel(mgr.GetClient())
		return err
	}); err != nil {
		return nil, err
	}

	r.Logger.Info("initial GPU cache", "models", r.NvidiaGpu)
	return r, nil
}

func NewNodeReconciler(mgr ctrl.Manager, parent *MonitorReconciler) (*NodeReconciler, error) {
	return &NodeReconciler{
		Client: mgr.GetClient(),
		Logger: ctrl.Log.WithName("controllers").WithName("Node"),
		parent: parent,
	}, nil
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			// 节点删除，清理 GPU 缓存
			r.parent.gpuMutex.Lock()
			delete(r.parent.NvidiaGpu, req.Name)
			r.parent.gpuMutex.Unlock()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 刷新 GPU 型号缓存（一次性拿全集更省 API 调用）
	models, err := gpu.GetNodeGpuModel(r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}
	r.parent.gpuMutex.Lock()
	r.parent.NvidiaGpu = models
	r.parent.gpuMutex.Unlock()
	r.Logger.Info("refreshed GPU cache", "size", len(models))
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(predicate.Or(
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
			predicate.GenerationChangedPredicate{},
		)).
		Complete(r)
}

func InitIndexField(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.PersistentVolumeClaim{}, "status.phase", func(rawObj client.Object) []string {
		pvc := rawObj.(*corev1.PersistentVolumeClaim)
		return []string{string(pvc.Status.Phase)}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbv1alpha1.Backup{}, "status.phase", func(rawObj client.Object) []string {
		backup := rawObj.(*kbv1alpha1.Backup)
		return []string{string(backup.Status.Phase)}
	}); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Service{}, "spec.type", func(rawObj client.Object) []string {
		svc := rawObj.(*corev1.Service)
		return []string{string(svc.Spec.Type)}
	})
}

func (r *MonitorReconciler) StartReconciler(ctx context.Context) error {
	r.startPeriodicReconcile(ctx)
	if r.TrafficClient != nil || r.ObjStorageClient != nil {
		r.startMonitorTraffic(ctx)
		r.startAsyncTrafficFlusher()
	}
	<-ctx.Done()
	r.stopPeriodicReconcile()
	return nil
}

func (r *MonitorReconciler) startPeriodicReconcile(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		waitNextMinuteWithDelay(ctx, trafficCollectDelay)
		ticker := time.NewTicker(r.periodicReconcile)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 传入上下文，方便在 processNamespaceList 里面用来取消等待信号
				r.enqueueNamespacesForReconcile(ctx)
			case <-r.stopCh:
				return
			case <-ctx.Done():
				// context 取消时退出
				return
			}
		}
	}()
}

func (r *MonitorReconciler) getNamespaceList(ctx context.Context) (*corev1.NamespaceList, error) {
	namespaceList := &corev1.NamespaceList{}
	req, err := labels.NewRequirement(userv1.UserLabelOwnerKey, selection.Exists, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create label requirement: %v", err)
	}
	return namespaceList, r.List(ctx, namespaceList, &client.ListOptions{
		LabelSelector: labels.NewSelector().Add(*req),
	})
}

func waitNextMinuteWithDelay(ctx context.Context, delay time.Duration) {
	waitTime := time.Until(time.Now().Truncate(time.Minute).Add(1*time.Minute)).Truncate(time.Second) + delay
	if waitTime <= 0 {
		return
	}
	logger.Info("wait for first reconcile", "waitTime", waitTime)
	select {
	case <-time.After(waitTime):
	case <-ctx.Done():
	}
}

func waitNextHour(ctx context.Context) {
	d := time.Until(time.Now().Truncate(time.Hour).Add(time.Hour))
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (r *MonitorReconciler) Close() {
	if r.RedisClient != nil {
		err := r.RedisClient.Close()
		if err != nil {
			return
		}
	}
}

func (r *MonitorReconciler) startMonitorTraffic(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		// 初始化窗口
		endTime := time.Now().UTC().Truncate(time.Minute).Add(-trafficWindowLead)
		startTime := endTime.Add(-trafficWindowLag)

		// 等待下个小时整点再执行首次监控
		waitNextMinuteWithDelay(ctx, trafficCollectDelay)
		waitNextHour(ctx)

		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		// 首次执行
		if err := r.MonitorTrafficUsed(startTime, endTime); err != nil {
			r.Logger.Error(err, "failed to monitor traffic on initial run", "startTime", startTime, "endTime", endTime)
		}

		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				// 使用超时保护，避免长时间阻塞
				done := make(chan struct{})
				go func(prevEnd time.Time) {
					defer close(done)
					newStart, newEnd := prevEnd, prevEnd.Add(1*time.Hour)
					if err := r.MonitorTrafficUsed(newStart, newEnd); err != nil {
						r.Logger.Error(err, "failed to monitor pod traffic used", "start", newStart, "end", newEnd)
					}
				}(endTime)

				select {
				case <-done:
					// 正常完成
					endTime = endTime.Add(1 * time.Hour)
					startTime = endTime.Add(-trafficWindowLag)
				case <-time.After(60 * time.Second):
					r.Logger.Error(fmt.Errorf("timeout"), "MonitorTrafficUsed timed out", "start", startTime, "end", endTime)
				}
			}
		}
	}()
}

func (r *MonitorReconciler) stopPeriodicReconcile() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *MonitorReconciler) enqueueNamespacesForReconcile(ctx context.Context) {
	r.Logger.Info("enqueue namespaces for reconcile", "time", time.Now().Format(time.RFC3339))

	namespaceList, err := r.getNamespaceList(ctx)
	if err != nil {
		r.Logger.Error(err, "failed to list namespaces")
		return
	}

	filterNormalNamespace(namespaceList)
	if err := r.processNamespaceList(ctx, namespaceList); err != nil {
		r.Logger.Error(err, "failed to process namespace", "time", time.Now().Format(time.RFC3339))
	}
}

func filterNormalNamespace(namespaceList *corev1.NamespaceList) {
	var items []corev1.Namespace
	for i := range namespaceList.Items {
		debtStatus := ""
		anno := namespaceList.Items[i].Annotations
		if anno != nil {
			debtStatus = anno[v1.DebtNamespaceAnnoStatusKey]
		}
		if debtStatus == v1.SuspendDebtNamespaceAnnoStatus || debtStatus == v1.SuspendCompletedDebtNamespaceAnnoStatus ||
			debtStatus == v1.FinalDeletionDebtNamespaceAnnoStatus || debtStatus == v1.FinalDeletionCompletedDebtNamespaceAnnoStatus {
			continue
		}
		items = append(items, namespaceList.Items[i])
	}
	namespaceList.Items = items
	logger.Info("filter normal namespace", "namespaceList len", len(namespaceList.Items), "time", time.Now().Format(time.RFC3339))
}

func (r *MonitorReconciler) processNamespaceList(ctx context.Context, namespaceList *corev1.NamespaceList) error {
	r.Logger.Info("start processNamespaceList", "namespaceList len", len(namespaceList.Items), "time", time.Now().Format(time.RFC3339))
	if len(namespaceList.Items) == 0 {
		r.Logger.Error(fmt.Errorf("no namespace to process"), "")
		return nil
	}
	if err := r.preMonitorResourceUsage(); err != nil {
		r.Logger.Error(err, "failed to pre monitor resource usage")
	}
	sem := semaphore.NewWeighted(concurrentLimit)
	wg := sync.WaitGroup{}
	wg.Add(len(namespaceList.Items))
	for i := range namespaceList.Items {
		go func(namespace *corev1.Namespace) {
			defer wg.Done()
			if err := sem.Acquire(ctx, 1); err != nil {
				r.Logger.Error(err, "Failed to acquire semaphore")
				return
			}
			defer sem.Release(1)
			if err := r.monitorResourceUsage(namespace); err != nil {
				r.Logger.Error(err, "monitor pod resource", "namespace", namespace.Name)
			}
		}(&namespaceList.Items[i])
	}
	wg.Wait()
	if err := r.monitorObjectStorageTraffic(); err != nil {
		r.Logger.Error(err, "failed to monitor object storage traffic")
	}
	r.Logger.Info("end processNamespaceList", "time", time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

func (r *MonitorReconciler) preMonitorResourceUsage() error {
	if r.ObjStorageMetricsClient != nil {
		metrics, err := objstorage.QueryUserUsageAndTraffic(r.ObjStorageMetricsClient)
		if err != nil {
			r.lastObjectMetrics = r.currentObjectMetrics
			return fmt.Errorf("failed to query object storage metrics: %w", err)
		}
		if r.currentObjectMetrics != nil {
			r.lastObjectMetrics = r.currentObjectMetrics
		} else {
			latestObjTrafficSentMetrics := make(objstorage.Metrics)
			startTime, endTime := time.Now().UTC().Add(-time.Hour), time.Now().UTC()
			traffic, err := r.DBClient.GetAllLatestObjTraffic(startTime, endTime)
			if err != nil {
				return fmt.Errorf("failed to get all latest object storage traffic: %w", err)
			}
			for i := range traffic {
				user := traffic[i].User
				bucket := traffic[i].Bucket
				if _, ok := metrics[user]; !ok {
					continue
				}

				if traffic[i].Time.Before(time.Now().Add(-time.Hour)) {
					continue
				}

				if _, ok := latestObjTrafficSentMetrics[user]; !ok {
					latestObjTrafficSentMetrics[user] = objstorage.MetricData{
						Sent: make(map[string]int64),
					}
				}

				latestObjTrafficSentMetrics[user].Sent[bucket] = traffic[i].TotalSent
			}
			r.lastObjectMetrics = latestObjTrafficSentMetrics
		}
		r.currentObjectMetrics = metrics
		logger.Info("success query object storage usage and traffic metrics", "time", time.Now().Format("2006-01-02 15:04:05"))
	}
	return nil
}

func (r *MonitorReconciler) monitorResourceUsage(namespace *corev1.Namespace) error {
	timeStamp := time.Now().UTC()
	resUsed := map[string]map[corev1.ResourceName]*quantity{}
	resNamed := make(map[string]*resources.ResourceNamed)

	instances, err := r.getInstances(namespace.Name)
	if err != nil {
		return fmt.Errorf("failed to get instances: %v", err)
	}

	podResMap := r.collectNamespacePodUsage(namespace.Name)

	for podName, podRes := range podResMap {
		resNameStr := fmt.Sprintf("pod/%s/%s", namespace.Name, podName)
		if resUsed[resNameStr] == nil {
			resUsed[resNameStr] = initResources()
		}
		for resName, qty := range podRes {
			resUsed[resNameStr][resName].Add(*qty.Quantity)
		}
		if _, exists := resNamed[resNameStr]; !exists {
			resNamed[resNameStr] = &resources.ResourceNamed{}
		}
	}
	if err := r.monitorPodResourceUsage(namespace.Name, resUsed, resNamed, instances); err != nil {
		return fmt.Errorf("failed to monitor pod resource usage: %v", err)
	}

	if err := r.monitorPVCResourceUsage(namespace.Name, resUsed, resNamed, instances); err != nil {
		return fmt.Errorf("failed to monitor PVC resource usage: %v", err)
	}

	if err := r.monitorDatabaseBackupUsage(namespace.Name, resUsed, resNamed); err != nil {
		return fmt.Errorf("failed to monitor backup resource usage: %v", err)
	}

	if err := r.monitorServiceResourceUsage(namespace.Name, resUsed, resNamed, instances); err != nil {
		return fmt.Errorf("failed to monitor service resource usage: %v", err)
	}

	if err := r.monitorObjectStorageUsage(namespace.Name, resUsed, resNamed); err != nil {
		return fmt.Errorf("failed to get object storage resource usage: %v", err)
	}

	var monitors []*resources.Monitor

	for name, podResource := range resUsed {
		isEmpty, used := r.getResourceUsed(podResource)
		if isEmpty {
			continue
		}
		monitors = append(monitors, &resources.Monitor{
			Category:   namespace.Name,
			Used:       used,
			Time:       timeStamp,
			Type:       resNamed[name].Type(),
			Name:       resNamed[name].Name(),
			ParentType: resNamed[name].ParentType(),
			ParentName: resNamed[name].ParentName(),
		})
	}
	return r.DBClient.InsertMonitor(context.Background(), monitors...)
}

func (r *MonitorReconciler) getInstances(namespace string) (map[string]struct{}, error) {
	instances := make(map[string]struct{})
	insList := metav1.PartialObjectMetadataList{}
	insList.SetGroupVersionKind(appv1.GroupVersion.WithKind("InstanceList"))
	if err := r.List(context.Background(), &insList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list instances: %v", err)
	}
	for i := range insList.Items {
		name := insList.Items[i].Labels[resources.AppStoreDeployLabelKey]
		if name == "" {
			name = insList.Items[i].Name
		}
		instances[name] = struct{}{}
	}
	return instances, nil
}

func (r *MonitorReconciler) monitorPodResourceUsage(namespace string, resUsed map[string]map[corev1.ResourceName]*quantity, resNamed map[string]*resources.ResourceNamed, instances map[string]struct{}) error {
	podList := &corev1.PodList{}
	if err := r.List(context.Background(), podList, &client.ListOptions{
		Namespace: namespace,
	}); err != nil {
		return fmt.Errorf("failed to list pods: %v", err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName == "" || (pod.Status.Phase == corev1.PodSucceeded && time.Since(pod.Status.StartTime.Time) > 1*time.Minute) {
			continue
		}
		podResNamed := resources.NewResourceNamed(pod)
		podResNamed.SetInstanceParent(instances)
		// 若该 Pod 已从缓存计入，避免重复统计
		if _, exists := resUsed[podResNamed.String()]; exists {
			// 仍然维护命名信息，防止后续getResourceUsed 取类型失败
			if _, ok := resNamed[podResNamed.String()]; !ok {
				resNamed[podResNamed.String()] = podResNamed
			}
			continue
		}
		resNamed[podResNamed.String()] = podResNamed
		if resUsed[podResNamed.String()] == nil {
			resUsed[podResNamed.String()] = initResources()
		}
		// skip pods that do not start for more than 1 minute
		skip := pod.Status.Phase != corev1.PodRunning && (pod.Status.StartTime == nil || time.Since(pod.Status.StartTime.Time) > 1*time.Minute)
		for _, container := range pod.Spec.Containers {
			// gpu only use limit and not ignore pod pending status
			if gpuRequest, ok := container.Resources.Limits[gpu.NvidiaGpuKey]; ok {
				if err := r.getGPUResourceUsage(pod, gpuRequest, resUsed[podResNamed.String()]); err != nil {
					r.Logger.Error(err, "get gpu resource usage failed", "pod", pod.Name)
				}
			}
			if skip {
				continue
			}
			if cpuRequest, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
				resUsed[podResNamed.String()][corev1.ResourceCPU].Add(cpuRequest)
			} else {
				resUsed[podResNamed.String()][corev1.ResourceCPU].Add(container.Resources.Requests[corev1.ResourceCPU])
			}
			if memoryRequest, ok := container.Resources.Limits[corev1.ResourceMemory]; ok {
				resUsed[podResNamed.String()][corev1.ResourceMemory].Add(memoryRequest)
			} else {
				resUsed[podResNamed.String()][corev1.ResourceMemory].Add(container.Resources.Requests[corev1.ResourceMemory])
			}
		}
	}
	return nil
}

func (r *MonitorReconciler) collectNamespacePodUsage(namespace string) map[string]map[corev1.ResourceName]*quantity {
	r.mu.Lock()
	defer r.mu.Unlock()
	nsUsageCopy := make(map[string]map[corev1.ResourceName]*quantity)
	if podMap, ok := r.podResUsageMap[namespace]; ok {
		for podName, resMap := range podMap {
			copied := initResources()
			for resName, qty := range resMap {
				copied[resName] = qty.DeepCopy() // Quantity 有 DeepCopy 方法，quantity需要你写一个
			}
			nsUsageCopy[podName] = copied
		}
	}
	return nsUsageCopy
}

func (q *quantity) DeepCopy() *quantity {
	if q == nil {
		return nil
	}
	clone := q.Quantity.DeepCopy() // clone 是值，不是指针
	return &quantity{
		Quantity: &clone, // 取地址，满足 *resource.Quantity
		detail:   q.detail,
	}
}

func (r *MonitorReconciler) monitorPVCResourceUsage(namespace string, resUsed map[string]map[corev1.ResourceName]*quantity, resNamed map[string]*resources.ResourceNamed, instances map[string]struct{}) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(context.Background(), pvcList, &client.ListOptions{
		Namespace:     namespace,
		FieldSelector: fields.OneTermEqualSelector("status.phase", string(corev1.ClaimBound)),
	}); err != nil {
		return fmt.Errorf("failed to list pvc: %v", err)
	}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if len(pvc.OwnerReferences) > 0 && pvc.OwnerReferences[0].Kind == "BackupRepo" {
			continue
		}
		pvcRes := resources.NewResourceNamed(pvc)
		pvcRes.SetInstanceParent(instances)
		if resUsed[pvcRes.String()] == nil {
			resNamed[pvcRes.String()] = pvcRes
			resUsed[pvcRes.String()] = initResources()
		}
		resUsed[pvcRes.String()][corev1.ResourceStorage].Add(pvc.Spec.Resources.Requests[corev1.ResourceStorage])
	}
	return nil
}

func (r *MonitorReconciler) monitorDatabaseBackupUsage(namespace string, resUsed map[string]map[corev1.ResourceName]*quantity, resNamed map[string]*resources.ResourceNamed) error {
	backupList := &kbv1alpha1.BackupList{}
	if err := r.List(context.Background(), backupList, &client.ListOptions{
		Namespace:     namespace,
		FieldSelector: fields.OneTermEqualSelector("status.phase", string(kbv1alpha1.BackupPhaseCompleted)),
	}); err != nil {
		return fmt.Errorf("failed to list backup: %v", err)
	}
	if len(backupList.Items) == 0 {
		return nil
	}
	for i := range backupList.Items {
		backup := &backupList.Items[i]
		backupRes := resources.NewResourceNamed(backup)
		//fmt.Printf("backup name: %v, backup size: %v, backupRes: %s \n", backupList.Items[i].Name, backupList.Items[i].Status.TotalSize, backupRes.String())
		if resUsed[backupRes.String()] == nil {
			resNamed[backupRes.String()] = backupRes
			resUsed[backupRes.String()] = initResources()
		}
		size := strings.TrimSpace(backup.Status.TotalSize)
		if size != "" {
			if q, err := resources.ParseCustomQuantity(size); err == nil {
				resUsed[backupRes.String()][corev1.ResourceStorage].Add(q)
			} else {
				r.Logger.Error(err, "parse custom quantity failed", "backup", backup)
			}
		}
	}
	return nil
}

// instance is the app instance name
func (r *MonitorReconciler) monitorServiceResourceUsage(namespace string, resUsed map[string]map[corev1.ResourceName]*quantity, resNamed map[string]*resources.ResourceNamed, instances map[string]struct{}) error {
	svcList := &corev1.ServiceList{}
	if err := r.List(context.Background(), svcList, &client.ListOptions{
		Namespace:     namespace,
		FieldSelector: fields.OneTermEqualSelector("spec.type", string(corev1.ServiceTypeNodePort)),
	}); err != nil {
		return fmt.Errorf("failed to list svc: %v", err)
	}
	for i := range svcList.Items {
		svc := &svcList.Items[i]
		if len(svc.Spec.Ports) == 0 {
			continue
		}
		port := make(map[int32]struct{})
		for _, svcPort := range svc.Spec.Ports {
			port[svcPort.NodePort] = struct{}{}
		}
		svcRes := resources.NewResourceNamed(svc)
		svcRes.SetInstanceParent(instances)
		if resUsed[svcRes.String()] == nil {
			resNamed[svcRes.String()] = svcRes
			resUsed[svcRes.String()] = initResources()
		}
		// nodeport 1:1000, the measurement is quantity 1000
		resUsed[svcRes.String()][corev1.ResourceServicesNodePorts].Add(
			*resource.NewQuantity(int64(1000*len(port)), resource.DecimalSI))
	}
	return nil
}

func (r *MonitorReconciler) getResourceUsed(podResource map[corev1.ResourceName]*quantity) (bool, map[uint8]int64) {
	used := map[uint8]int64{}
	isEmpty := true
	for i := range podResource {
		if podResource[i].MilliValue() == 0 {
			continue
		}
		isEmpty = false
		if pType, ok := r.Properties.StringMap[i.String()]; ok {
			used[pType.Enum] = int64(math.Ceil(float64(podResource[i].MilliValue()) / float64(pType.Unit.MilliValue())))
			continue
		}
		r.Logger.Error(fmt.Errorf("not found resource type"), "resource", i.String())
	}
	return isEmpty, used
}

func (r *MonitorReconciler) monitorObjectStorageUsage(namespace string, resMap map[string]map[corev1.ResourceName]*quantity, namedMap map[string]*resources.ResourceNamed) error {
	username := config.GetUserNameByNamespace(namespace)
	if r.currentObjectMetrics == nil {
		return nil
	}
	metric, ok := r.currentObjectMetrics[username]
	if !ok || metric.Usage == nil {
		return nil
	}
	for bucket, usage := range r.currentObjectMetrics[username].Usage {
		if bucket == "" || usage <= 0 {
			continue
		}
		objStorageNamed := resources.NewObjStorageResourceNamed(bucket)
		namedMap[objStorageNamed.String()] = objStorageNamed
		if _, ok := resMap[objStorageNamed.String()]; !ok {
			resMap[objStorageNamed.String()] = initResources()
		}
		resMap[objStorageNamed.String()][corev1.ResourceStorage].Add(*resource.NewQuantity(usage, resource.BinarySI))
	}
	return nil
}

func (r *MonitorReconciler) monitorObjectStorageTraffic() error {
	if r.currentObjectMetrics == nil {
		return nil
	}
	var objTraffic []*types.ObjectStorageTraffic
	now := time.Now().UTC()

	// 滑动窗口：保证数据覆盖
	endTime := time.Now().UTC().Truncate(time.Minute).Add(-trafficWindowLead)
	startTime := endTime.Add(-trafficWindowLag)

	logger.Info("monitor object storage traffic",
		"startTime", startTime.Format(time.RFC3339),
		"endTime", endTime.Format(time.RFC3339))

	for user, metric := range r.currentObjectMetrics {
		if len(metric.Sent) == 0 {
			continue
		}
		for bucket, m := range metric.Sent {
			// 使用 Redis 缓存计算 delta
			delta := r.TrafficCache.Delta(context.Background(), user, bucket, m)

			// 更新缓存
			_ = r.TrafficCache.SaveTraffic(context.Background(), user, bucket, m)
			objTraffic = append(objTraffic, &types.ObjectStorageTraffic{
				Time:      now,
				User:      user,
				Bucket:    bucket,
				TotalSent: m,
				Sent:      delta,
			})
		}
	}
	if len(objTraffic) != 0 {
		if err := r.DBClient.SaveObjTraffic(objTraffic...); err != nil {
			return fmt.Errorf("failed to save object storage traffic: %w", err)
		}
	}

	return nil
}

func (r *MonitorReconciler) startAsyncTrafficFlusher() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flushStableObjectTrafficToDB() // 异步补偿写入数据库
			case <-r.stopCh:
				return
			}
		}
	}()
}

func (r *MonitorReconciler) flushStableObjectTrafficToDB() {
	ctx := context.Background()
	keys := r.scanKeys(ctx, "traffic:object:*")

	var traffics []*types.ObjectStorageTraffic
	now := time.Now().UTC()

	for _, key := range keys {
		vals, err := r.RedisClient.HGetAll(ctx, key).Result()
		if err != nil {
			r.Logger.Error(err, "Redis HGetAll failed", "key", key)
			continue
		}
		if len(vals) == 0 {
			continue
		}

		ts, err := time.Parse(time.RFC3339, vals["timestamp"])
		if err != nil {
			r.Logger.Error(err, "invalid timestamp format in Redis", "key", key, "value", vals["timestamp"])
			continue
		}
		if now.Sub(ts) < 90*time.Second {
			// 数据不稳定，跳过
			continue
		}

		current, err1 := strconv.ParseInt(vals["current"], 10, 64)
		previous, err2 := strconv.ParseInt(vals["previous"], 10, 64)
		if err1 != nil || err2 != nil {
			r.Logger.Error(fmt.Errorf("parse int error"), "current/previous parse failed", "key", key, "current", vals["current"], "previous", vals["previous"])
			continue
		}

		sent := int64(math.Max(0, float64(current-previous)))

		parts := strings.Split(key, ":")
		if len(parts) < 4 {
			r.Logger.Info("unexpected Redis key format", "key", key)
			continue
		}
		user, bucket := parts[2], parts[3]

		traffics = append(traffics, &types.ObjectStorageTraffic{
			Time:      ts,
			User:      user,
			Bucket:    bucket,
			TotalSent: current,
			Sent:      sent,
		})
	}

	if len(traffics) > 0 {
		if err := r.DBClient.SaveObjTraffic(traffics...); err != nil {
			r.Logger.Error(err, "flushStableObjectTrafficToDB: SaveObjTraffic failed", "count", len(traffics))
		} else {
			r.Logger.Info("flushStableObjectTrafficToDB: flushed traffic", "count", len(traffics))
		}
	}
}

func (r *MonitorReconciler) scanKeys(ctx context.Context, pattern string) []string {
	var cursor uint64
	var keys []string
	for {
		batch, next, err := r.RedisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			r.Logger.Error(err, "error scanning redis keys", "pattern", pattern, "cursor", cursor)
			break
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys
}

func (tc *TrafficCache) SaveTraffic(ctx context.Context, user, bucket string, current int64) error {
	key := fmt.Sprintf("traffic:object:%s:%s", user, bucket)
	prevVal := current
	if val, err := tc.Redis.HGet(ctx, key, "current").Result(); err == nil {
		if pv, errParse := strconv.ParseInt(val, 10, 64); errParse == nil {
			prevVal = pv
		}
	}
	pipe := tc.Redis.TxPipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"previous":  prevVal,
		"current":   current,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
	pipe.Expire(ctx, key, tc.TTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Error(err, "[Redis] SaveTraffic failed", "key", key)
	}
	return err
}

func (r *MonitorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&corev1.Pod{}).Complete(r)
}

func (r *MonitorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			// 删除 Pod 时同步回收命名空间级缓存
			r.mu.Lock()
			if nsPods, exists := r.podResUsageMap[req.Namespace]; exists {
				delete(nsPods, req.Name)
				if len(nsPods) == 0 {
					delete(r.podResUsageMap, req.Namespace)
				}
			}
			r.mu.Unlock()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	podResUsage := r.calculatePodResource(pod)

	r.mu.Lock()
	nsMap, ok := r.podResUsageMap[pod.Namespace]
	if !ok {
		nsMap = make(map[string]map[corev1.ResourceName]*quantity)
		r.podResUsageMap[pod.Namespace] = nsMap
	}
	nsMap[pod.Name] = podResUsage
	r.mu.Unlock()

	return ctrl.Result{}, nil
}

func (r *MonitorReconciler) calculatePodResource(pod *corev1.Pod) map[corev1.ResourceName]*quantity {
	usage := initResources()

	if pod.Spec.NodeName == "" ||
		(pod.Status.Phase == corev1.PodSucceeded && time.Since(pod.Status.StartTime.Time) > 1*time.Minute) {
		return usage
	}

	skip := pod.Status.Phase != corev1.PodRunning && (pod.Status.StartTime == nil || time.Since(pod.Status.StartTime.Time) > 1*time.Minute)

	for _, c := range pod.Spec.Containers {
		if gpuReq, ok := c.Resources.Limits[gpu.NvidiaGpuKey]; ok {
			// gpu only use limit
			usage[gpu.NvidiaGpuKey].Add(gpuReq)
		}
		if skip {
			continue
		}
		if cpuLimit, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			usage[corev1.ResourceCPU].Add(cpuLimit)
		} else if cpuReq, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			usage[corev1.ResourceCPU].Add(cpuReq)
		}
		if memLimit, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			usage[corev1.ResourceMemory].Add(memLimit)
		} else if memReq, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			usage[corev1.ResourceMemory].Add(memReq)
		}
	}

	return usage
}

func (tc *TrafficCache) Delta(ctx context.Context, user, bucket string, current int64) int64 {
	key := fmt.Sprintf("traffic:object:%s:%s", user, bucket)
	val, err := tc.Redis.HGet(ctx, key, "current").Result()
	if err != nil {
		logger.Info("[Redis] Delta cold start -> 0", "key", key, "err", err)
		return 0
	}
	prev, errParse := strconv.ParseInt(val, 10, 64)
	if errParse != nil {
		logger.Info("[Redis] Delta parse failed -> 0", "key", key, "err", errParse)
		return 0
	}
	if prev == 0 {
		return 0
	}
	if current < prev {
		return 0
	}
	return current - prev
}

func (r *MonitorReconciler) MonitorTrafficUsed(startTime, endTime time.Time) error {
	logger.Info("start getTrafficUsed", "startTime", startTime.Format(time.RFC3339), "endTime", endTime.Format(time.RFC3339))
	execTime := time.Now().UTC()
	if r.TrafficClient != nil {
		if err := r.monitorPodTrafficUsed(startTime, endTime); err != nil {
			r.Logger.Error(err, "failed to monitor pod traffic used")
		}
	}
	if r.ObjStorageClient != nil {
		if err := r.monitorObjectStorageTrafficUsed(startTime, endTime); err != nil {
			r.Logger.Error(err, "failed to monitor object storage traffic used")
		}
	}
	r.Logger.Info("success to monitor pod traffic used", "startTime", startTime.Format(time.RFC3339), "endTime", endTime.Format(time.RFC3339), "execTime", time.Since(execTime).String())
	return nil
}

func (r *MonitorReconciler) monitorObjectStorageTrafficUsed(startTime, endTime time.Time) error {
	buckets, err := r.DBClient.GetTimeObjBucketBucket(startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to get object storage buckets: %w", err)
	}
	r.Logger.Info("object storage buckets", "buckets len", len(buckets))
	wg, _ := errgroup.WithContext(context.Background())
	wg.SetLimit(10)
	for i := range buckets {
		bucket := buckets[i]
		if !strings.Contains(bucket, "-") {
			continue
		}
		wg.Go(func() error {
			return r.handlerObjectStorageTrafficUsed(startTime, endTime, bucket)
		})
	}
	return wg.Wait()
}

func (r *MonitorReconciler) handlerObjectStorageTrafficUsed(startTime, endTime time.Time, bucket string) error {
	bytes, err := r.DBClient.HandlerTimeObjBucketSentTraffic(startTime, endTime, bucket)
	if err != nil {
		return fmt.Errorf("failed to get object storage flow: %w", err)
	}
	// Because the obtained traffic includes traffic communicating with the controller, filter out traffic smaller than 1 MB
	if bytes < 1024*1024 {
		return nil
	}
	unit := r.Properties.StringMap[resources.ResourceNetwork].Unit
	used := int64(math.Ceil(float64(resource.NewQuantity(bytes, resource.BinarySI).MilliValue()) / float64(unit.MilliValue())))

	namespace := "ns-" + strings.SplitN(bucket, "-", 2)[0]
	ro := resources.Monitor{
		Category: namespace,
		Name:     bucket,
		Used:     map[uint8]int64{r.Properties.StringMap[resources.ResourceNetwork].Enum: used},
		Time:     endTime.Add(-1 * time.Minute),
		Type:     resources.AppType[resources.ObjectStorage],
	}
	r.Logger.Info("object storage traffic used", "monitor", ro)
	err = r.DBClient.InsertMonitor(context.Background(), &ro)
	if err != nil {
		return fmt.Errorf("failed to insert monitor: %w", err)
	}
	return nil
}

func (r *MonitorReconciler) monitorPodTrafficUsed(startTime, endTime time.Time) error {
	monitors, err := r.DBClient.GetDistinctMonitorCombinations(startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to get distinct monitor combinations: %w", err)
	}
	r.Logger.Info("distinct monitor combinations", "monitors len", len(monitors))
	wg, _ := errgroup.WithContext(context.Background())
	wg.SetLimit(100)
	for i := range monitors {
		monitor := monitors[i]
		wg.Go(func() error {
			return r.handlerTrafficUsed(startTime, endTime, monitor)
		})
	}
	return wg.Wait()
}

func (r *MonitorReconciler) handlerTrafficUsed(startTime, endTime time.Time, monitor resources.Monitor) error {
	bytes, err := r.TrafficClient.GetTrafficSentBytes(startTime, endTime, monitor.Category, monitor.Type, monitor.Name)
	if err != nil {
		return fmt.Errorf("failed to get traffic sent bytes: %w", err)
	}
	unit := r.Properties.StringMap[resources.ResourceNetwork].Unit
	used := int64(math.Ceil(float64(resource.NewQuantity(bytes, resource.BinarySI).MilliValue()) / float64(unit.MilliValue())))
	if used == 0 {
		return nil
	}
	//logger.Info("traffic used ", "monitor", monitor, "used", used, "unit", unit, "bytes", bytes)
	ro := resources.Monitor{
		Category: monitor.Category,
		Name:     monitor.Name,
		Used:     map[uint8]int64{r.Properties.StringMap[resources.ResourceNetwork].Enum: used},
		Time:     endTime.Add(-1 * time.Minute),
		Type:     monitor.Type,
	}
	err = r.DBClient.InsertMonitor(context.Background(), &ro)
	if err != nil {
		return fmt.Errorf("failed to insert monitor: %w", err)
	}
	return nil
}

func (r *MonitorReconciler) getGPUResourceUsage(pod *corev1.Pod, gpuReq resource.Quantity, rs map[corev1.ResourceName]*quantity) (err error) {
	nodeName := pod.Spec.NodeName
	r.gpuMutex.Lock()
	defer r.gpuMutex.Unlock()
	gpuModel, exist := r.NvidiaGpu[nodeName]
	if !exist {
		if r.NvidiaGpu, err = gpu.GetNodeGpuModel(r.Client); err != nil {
			return fmt.Errorf("get node gpu model failed: %w", err)
		}
		if gpuModel, exist = r.NvidiaGpu[nodeName]; !exist {
			return fmt.Errorf("node %s not found gpu model", nodeName)
		}
	}
	if _, ok := rs[resources.NewGpuResource(gpuModel.GpuInfo.GpuProduct)]; !ok {
		rs[resources.NewGpuResource(gpuModel.GpuInfo.GpuProduct)] = initGpuResources()
	}
	logger.Info("gpu request", "pod", pod.Name, "namespace", pod.Namespace, "gpu req", gpuReq.String(), "node", nodeName, "gpu model", gpuModel.GpuInfo.GpuProduct)
	rs[resources.NewGpuResource(gpuModel.GpuInfo.GpuProduct)].Add(gpuReq)
	return nil
}

func initResources() (rs map[corev1.ResourceName]*quantity) {
	rs = make(map[corev1.ResourceName]*quantity)
	rs[corev1.ResourceCPU] = &quantity{Quantity: resource.NewQuantity(0, resource.DecimalSI)}
	rs[corev1.ResourceMemory] = &quantity{Quantity: resource.NewQuantity(0, resource.BinarySI)}
	rs[corev1.ResourceStorage] = &quantity{Quantity: resource.NewQuantity(0, resource.BinarySI)}
	rs[resources.ResourceNetwork] = &quantity{Quantity: resource.NewQuantity(0, resource.DecimalSI)}
	rs[corev1.ResourceServicesNodePorts] = &quantity{Quantity: resource.NewQuantity(0, resource.DecimalSI)}
	return rs
}

func initGpuResources() *quantity {
	return &quantity{Quantity: resource.NewQuantity(0, resource.DecimalSI), detail: ""}
}

func (r *MonitorReconciler) DropMonitorCollectionOlder() error {
	return r.DBClient.DropMonitorCollectionsOlderThan(30)
}

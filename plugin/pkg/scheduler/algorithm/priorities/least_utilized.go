/*
Copyright 2017 The Kubernetes Authors.

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

package priorities

import (
	"fmt"
	"sync"
	"time"

	metrics_api "k8s.io/heapster/metrics/apis/metrics/v1alpha1"

	"k8s.io/kubernetes/pkg/api"
	api_v1 "k8s.io/kubernetes/pkg/api/v1"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/kubectl/metricsutil"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"

	"github.com/golang/glog"
)

type LeastUtilized struct {
	metricsCache *MetricsCache
}

func NewLeastUtilizedPriority(metricsCache *MetricsCache) (algorithm.PriorityMapFunction, algorithm.PriorityReduceFunction) {
	leastUtilized := &LeastUtilized{
		metricsCache: metricsCache,
	}
	return leastUtilized.CalculateLeastUtilizedPriority, nil
}

func (l *LeastUtilized) CalculateLeastUtilizedPriority(pod *api.Pod, meta interface{}, nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}

	allocatableResources := nodeInfo.AllocatableResource()
	nodeMetrics := l.metricsCache.GetForNode(node.Name)
	if nodeMetrics == nil {
		glog.V(4).Infof("No resource usage metrics for node %v", node.Name)
		// Nodes with unknown resource usage are least preferred.
		return schedulerapi.HostPriority{
			Host:  node.Name,
			Score: 0,
		}, nil
	}

	cpuUsage := nodeMetrics.Usage[api_v1.ResourceCPU]
	memUsage := nodeMetrics.Usage[api_v1.ResourceMemory]
	cpuScore := calculateUtilizationScore(cpuUsage.MilliValue(), allocatableResources.MilliCPU, node.Name)
	memScore := calculateUtilizationScore(memUsage.Value(), allocatableResources.Memory, node.Name)

	glog.V(4).Infof(
		"%v -> %v: Least Utilized Priority, capacity %d millicores %d memory bytes, usage %d millicores %d memory bytes, score %d CPU %d memory",
		pod.Name, node.Name,
		allocatableResources.MilliCPU, allocatableResources.Memory,
		cpuUsage, memUsage,
		cpuScore, memScore,
	)

	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: int((cpuScore+memScore) / 2),
	}, nil
}

func calculateUtilizationScore(usage int64, capacity int64, node string) int64 {
	if capacity == 0 {
		glog.V(4).Infof("No capacity on node %s", node)
		return 0
	}
	if usage > capacity {
		glog.V(4).Infof("Combined resource usage %d exceeds capacity %d on node %s", usage, capacity, node)
		return 0
	}
	return (capacity - usage) * 10 / capacity
}

func NewMetricsCache(client clientset.Interface) *MetricsCache {
	cache := &MetricsCache{
		heapsterClient: metricsutil.DefaultHeapsterMetricsClient(client.Core()),
		metrics: map[string]metrics_api.NodeMetrics{},
		updateInterval: 30*time.Second,
	}
	go cache.Run()
	return cache
}

type MetricsCache struct {
	heapsterClient *metricsutil.HeapsterMetricsClient
	metrics map[string]metrics_api.NodeMetrics
	updateInterval time.Duration
	mutex sync.Mutex
}

func (cache *MetricsCache) Run() {
	for {
		select {
		case <-time.After(cache.updateInterval):
			{
				metrics, err := cache.heapsterClient.GetNodeMetrics("", labels.Everything())
				if err != nil {
					glog.Warningf("Error while listing metrics: %v", err)
					continue
				}
				newMetricsMap := map[string]metrics_api.NodeMetrics{}
				for _, m := range metrics {
					newMetricsMap[m.Name] = m
				}
				cache.mutex.Lock()
				defer cache.mutex.Unlock()
				cache.metrics = newMetricsMap
			}
		}
	}
}

func (cache *MetricsCache) GetForNode(name string) *metrics_api.NodeMetrics {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if m, found := cache.metrics[name]; found {
		return &m
	}
	return nil
}


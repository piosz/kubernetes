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

package podclassifier

import (
	"io"

	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"

	"k8s.io/kubernetes/pkg/admission"
	"k8s.io/kubernetes/pkg/api"
	apierrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/kubelet/qos"
	"k8s.io/kubernetes/plugin/pkg/scheduler/factory"
)

func init() {
	admission.RegisterPlugin("PodClassifier", func(client clientset.Interface, config io.Reader) (admission.Interface, error) {
		return NewPodClassifier(), nil
	})
}

type podClassifier struct{}

func (podClassifier) Admit(a admission.Attributes) (err error) {
	// Ignore all calls to subresources or resources other than pods.
	if a.GetSubresource() != "" || a.GetResource().GroupResource() != api.Resource("pods") {
		return nil
	}
	pod, ok := a.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	if qos.BestEffort == qos.GetPodQOS(pod) {
		pod.Annotations[factory.SchedulerAnnotationKey] = api.BestEffortSchedulerName
	} else {
		pod.Annotations[factory.SchedulerAnnotationKey] = api.DefaultSchedulerName
	}

	return nil
}

func (podClassifier) Handles(operation admission.Operation) bool {
	return operation == admission.Create
}

func NewPodClassifier() admission.Interface {
	return new(podClassifier)
}

REGISTRY = gcr.io/psz-dev
TAG = dev

container:
	cp ../../../_output/local/go/bin/kube-scheduler .
	docker build --pull -t ${REGISTRY}/best-effort-scheduler:$(TAG) .

push: container
	gcloud docker push ${REGISTRY}/best-effort-scheduler:$(TAG)

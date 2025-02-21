kind-build-and-load:
	./scripts/kind-build-and-load.sh

deploy-example:
	kubectl apply -f examples/k8s/deployment.yaml

reload-example:
	kubectl rollout restart deployment gitea-cdevents-adapter

delete-example:
	kubectl delete -f examples/k8s/deployment.yaml

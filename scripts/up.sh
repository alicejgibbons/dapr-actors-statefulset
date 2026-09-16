#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME=dapr-actors-statefulset
NAMESPACE=actors-demo
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Creating kind cluster '$CLUSTER_NAME' (if it doesn't already exist)"
if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME" --config "$REPO_ROOT/kind-config.yaml"
else
  echo "    cluster already exists, skipping"
fi
kubectl config use-context "kind-$CLUSTER_NAME"

echo "==> Installing Dapr control plane"
dapr init -k --wait --timeout 600

echo "==> Installing KEDA"
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update kedacore >/dev/null
helm upgrade --install keda kedacore/keda \
  --namespace keda-system --create-namespace \
  --wait --timeout 5m

echo "==> Building images"
docker build -t dapr-actors-statefulset/actor-host:local -f "$REPO_ROOT/src/ActorHost/Dockerfile" "$REPO_ROOT"
docker build -t dapr-actors-statefulset/actor-client:local -f "$REPO_ROOT/src/ActorClient/Dockerfile" "$REPO_ROOT"

echo "==> Loading images into kind cluster"
kind load docker-image dapr-actors-statefulset/actor-host:local --name "$CLUSTER_NAME"
kind load docker-image dapr-actors-statefulset/actor-client:local --name "$CLUSTER_NAME"

echo "==> Applying Kubernetes manifests"
for f in $(ls "$REPO_ROOT"/k8s/*.yaml | sort); do
  echo "    kubectl apply -f $f"
  kubectl apply -f "$f"
done

echo "==> Waiting for rollout"
kubectl -n "$NAMESPACE" rollout status statefulset/actor-host --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/actor-client --timeout=180s

echo
echo "==> Ready. See README.md for the demo walkthrough:"
echo "    kubectl get pods -n $NAMESPACE -w"
echo "    kubectl logs -n $NAMESPACE deploy/actor-client -f"

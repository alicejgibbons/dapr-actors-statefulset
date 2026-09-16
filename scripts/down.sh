#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME=dapr-actors-statefulset

echo "==> Deleting kind cluster '$CLUSTER_NAME'"
kind delete cluster --name "$CLUSTER_NAME"

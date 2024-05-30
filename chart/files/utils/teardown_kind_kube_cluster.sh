#!/bin/bash -e

KIND_CLUSTER_NAME="${2:-nrtsearch-kind}"
KUBERNETES_DASHBOARD_ACCESS_PORT="${3:-43101}"
NRTSEARCH_NAMEPSACE="${4:-nrtsearch}"

# Script to check the alias output
shopt -s expand_aliases

alias helm="docker run -ti --rm -v $(pwd):/apps -w /apps \
    -v ~/.kube:/root/.kube -v ~/.helm:/root/.helm -v ~/.config/helm:/root/.config/helm \
    -v ~/.cache/helm:/root/.cache/helm \
    --network=host \
    alpine/helm:3.11.1"

#helm uninstall release
helm uninstall $NRTSEARCH_NAMEPSACE-release --namespace $NRTSEARCH_NAMEPSACE

# remove nrtsearh namespace
kubectl delete namespace $NRTSEARCH_NAMEPSACE

# remove kubectl proxy port process
kill -9 $(lsof -t -i:${KUBERNETES_DASHBOARD_ACCESS_PORT})

#remove kind cluster
kind delete cluster --name $KIND_CLUSTER_NAME

#!/bin/bash -e

KIND_CLUSTER_NAME="${2:-nrtsearch-kind}"
NRTSEARCH_NAMEPSACE="${4:-nrtsearch}"

# Script to check the alias output
shopt -s expand_aliases

alias helm="docker run -ti --rm -v $(pwd):/apps -w /apps \
    -v ~/.kube:/root/.kube -v ~/.helm:/root/.helm -v ~/.config/helm:/root/.config/helm \
    -v ~/.cache/helm:/root/.cache/helm \
    --network=host \
    alpine/helm:3.11.1"

# Helm install release
helm upgrade --install --create-namespace -n $NRTSEARCH_NAMEPSACE $NRTSEARCH_NAMEPSACE-release --values chart/values.yaml chart/

echo -e "Installed nrtsearch release successfully!!"

#!/bin/bash -e

SCRIPT_PATH=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
APP_DOCKER_IMAGE_TAG="${1:-nrtsearch-dev-user:latest}"
KIND_CLUSTER_NAME="${2:-nrtsearch-kind}"
KUBERNETES_DASHBOARD_ACCESS_PORT="${3:-43101}"
NRTSEARCH_NAMEPSACE="${4:-nrtsearch}"
KIND_CLUSTER_CONFIG="${SCRIPT_PATH}/kind/multi-node-config.yaml"

echo -e "NOTE: \n 1. This script requires kind and kubectl to be installed locally on the system. \n\n"

KUBECTL_PROXY_PID=""

setup_kubernetes_dashboard () {
  kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.6.1/aio/deploy/recommended.yaml

  kubectl proxy -p $KUBERNETES_DASHBOARD_ACCESS_PORT &>/dev/null &
  KUBECTL_PROXY_PID="$!"
  echo -e "Kubectl proxy for accessing kubernetes dashboard at the port $KUBERNETES_DASHBOARD_ACCESS_PORT has pid : $KUBECTL_PROXY_PID\n"
  sleep 5
  # enable guest access to the dashboard
  kubectl patch deployment kubernetes-dashboard -n kubernetes-dashboard --type 'json' -p '[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--enable-skip-login"}]'
  # create and associate cluster admin account for the dashboard
  kubectl create clusterrolebinding kubernetes-dashboard-guest --clusterrole=cluster-admin --serviceaccount=kubernetes-dashboard:kubernetes-dashboard
  sleep 5
  echo -e "kubernetes dashboard is available at http:/$(hostname -i):$KUBERNETES_DASHBOARD_ACCESS_PORT/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
}

setup_kind_kube_cluster () {
  kind create cluster --config $KIND_CLUSTER_CONFIG

  # Load the app docker image in kind cluster
  kind load docker-image $APP_DOCKER_IMAGE_TAG --name $KIND_CLUSTER_NAME
}

## Step-1 Build and publish docker image locally
echo -e "Step-1 Build and publishing nrtsearch's docker image locally."
chmod 755 ./gradlew
docker build -t $APP_DOCKER_IMAGE_TAG .

## Step-2 Create kind kubernetes cluster
echo -e "Step-2 Create kind kubernetes cluster"
setup_kind_kube_cluster

## Step-3 Setup kubernetes dashboard
echo -e "Step-3 Setup kubernetes dashboard"
setup_kubernetes_dashboard

echo -e "\n\nOne more thing!! If this script is executed other than your local machine, then you would need to be able to access the remote machine's port : $KUBERNETES_DASHBOARD_ACCESS_PORT in order to access Kubernetes dashboard from your local browser"
echo -e "A simple way of achieving this to execute the following command is to execute the following command in a separate terminal on your local\n\n"
echo -e "ssh -C -N -L 0.0.0.0:$KUBERNETES_DASHBOARD_ACCESS_PORT:127.0.0.1:$KUBERNETES_DASHBOARD_ACCESS_PORT <username>@<remote_machine> \n\n"
echo -e "Now you can access the Kubernetes dashboard in your browser and then access the following url: \n"
echo -e "http://localhost:$KUBERNETES_DASHBOARD_ACCESS_PORT/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/\n"

echo -e "Now you are ready to deploy your helm charts...\n"

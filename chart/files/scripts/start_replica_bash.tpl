#!/bin/bash -e

#enable job control
set -m

echo "Starting up replica"

# update the hostname for replica
echo -e "\nhostName: \"`hostname -i`\" \n `cat configs/lucene_server_configuration_replica.yaml  | grep -v hostName`" > /user/app/lucene_server_configuration_replica.yaml

{{ if .Values.exploreNrtsearchMode.enabled }}
/user/app/build/install/nrtsearch/bin/lucene-server /user/app/lucene_server_configuration_replica.yaml &

apt update && apt install -y curl

sleep 3

echo "check and wait until primary pod is up"
PRIMARY_STATUS=""
until [ "$PRIMARY_STATUS" == "Started" ]
do
  PRIMARY_CHECK=$(curl "http://{{ include "nrtsearch.primary.service.name" . }}{{ include "service.name.suffix" . }}:{{ .Values.primaryMainPort }}" 2>&1)
  if [[ "$PRIMARY_CHECK" == *"Received HTTP/0.9 when not allowed"* ]]; then
    PRIMARY_STATUS="Started"
  fi
  echo "waiting for primary to be up before trying to register, sleeping for 5 seconds"
  sleep 5
done

echo "checking if index on primary pod is started"
INDEX_CHECK=""
until [ "$INDEX_CHECK" == "{{ .Values.exploreNrtsearchMode.testIndexName }}" ]
do
    INDEX_CHECK=$(./build/install/nrtsearch/bin/lucene-client -h {{ include "nrtsearch.primary.service.name" . }}{{ include "service.name.suffix" . }} -p {{ .Values.primaryMainPort }} indices | xargs)
    echo "Checking if the index: {{ .Values.exploreNrtsearchMode.testIndexName }} in the primary is created, sleeping for 5 seconds"
    sleep 5
done

./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.replicaMainPort }} createIndex --indexName  {{ .Values.exploreNrtsearchMode.testIndexName }}
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.replicaMainPort }} settings -f /user/app/configs/settings_replica.json
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.replicaMainPort }} registerFields -f /user/app/configs/registerFields.json
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.replicaMainPort }} startIndex -f /user/app/configs/startIndex_replica.json

fg
{{ else }}
/user/app/build/install/nrtsearch/bin/lucene-server /user/app/lucene_server_configuration_replica.yaml
{{ end }}

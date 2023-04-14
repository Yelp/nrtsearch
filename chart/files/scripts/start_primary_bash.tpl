#!/bin/bash -e

#enable job control
set -m

echo "Starting up primary"

{{ if .Values.exploreNrtsearchMode.enabled }}
/user/app/build/install/nrtsearch/bin/lucene-server /user/app/configs/lucene_server_configuration_primary.yaml &

sleep 15

./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.primaryMainPort }} createIndex --indexName {{ .Values.exploreNrtsearchMode.testIndexName }}
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.primaryMainPort }} settings -f /user/app/configs/settings_primary.json
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.primaryMainPort }} registerFields -f /user/app/configs/registerFields.json
./build/install/nrtsearch/bin/lucene-client -h `hostname -i` -p {{ .Values.primaryMainPort }} startIndex -f /user/app/configs/startIndex_primary.json

fg
{{ else }}
/user/app/build/install/nrtsearch/bin/lucene-server /user/app/configs/lucene_server_configuration_primary.yaml
{{ end }}

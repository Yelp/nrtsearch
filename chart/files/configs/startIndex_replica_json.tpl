{
  "indexName" : "{{ .Values.exploreNrtsearchMode.testIndexName }}",
  "mode": 2,
  "primaryAddress": "{{ include "nrtsearch.primary.service.name" . }}{{ include "service.name.suffix" . }}",
  "port": {{ .Values.primaryReplicationPort }}
}

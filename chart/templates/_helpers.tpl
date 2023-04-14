{{/*
Name of the app.
*/}}
{{- define "nrtsearch.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "nrtsearch.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "nrtsearch.serviceAccountName" -}}
{{- default .Chart.Name "nrtsearch-user" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Cluster DNS suffix
*/}}
{{- define "service.name.suffix" -}}
{{- default .Chart.Name ".nrtsearch.svc.cluster.local" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Primary's service name
*/}}
{{- define "nrtsearch.primary.service.name" -}}
{{- default .Chart.Name "nrtsearch-primary-svc" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Replica's service name
*/}}
{{- define "nrtsearch.replica.service.name" -}}
{{- default .Chart.Name "nrtsearch-replica-svc" | trunc 63 | trimSuffix "-" }}
{{- end }}

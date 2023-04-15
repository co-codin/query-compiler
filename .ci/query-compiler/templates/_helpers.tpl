{{/*
Expand the name of the chart.
*/}}
{{- define "query-compiler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "query-compiler.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "query-compiler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Extract environment from release name with fallback
*/}}
{{- define "chart.environment" -}}
{{- $parts := split "-" .Release.Name -}}
{{- if gt (len $parts) 2 -}}
  {{- $lastIndex := sub (len $parts) 1 -}}
  {{- $env := index $parts (printf "%d" $lastIndex) -}}
  {{- printf "%s" $env -}}
{{- else -}}
  {{- printf "unknown" -}}
{{- end -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "query-compiler.labels" -}}
helm.sh/chart: {{ include "query-compiler.chart" . }}
{{ include "query-compiler.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
environment: {{ include "chart.environment" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "query-compiler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "query-compiler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "query-compiler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "query-compiler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Common template helpers for the shelf chart.
*/}}

{{- define "shelf.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shelf.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "shelf.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shelf.labels" -}}
helm.sh/chart: {{ include "shelf.chart" . }}
{{ include "shelf.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: shelf
{{- with .Values.podLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "shelf.selectorLabels" -}}
app.kubernetes.io/name: {{ include "shelf.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "shelf.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "shelf.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "shelf.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "shelf.priorityClassName" -}}
{{- if .Values.priorityClass.create -}}
{{- default (printf "%s-data-plane" (include "shelf.fullname" .)) .Values.priorityClassName -}}
{{- else -}}
{{- .Values.priorityClassName | default "" -}}
{{- end -}}
{{- end -}}

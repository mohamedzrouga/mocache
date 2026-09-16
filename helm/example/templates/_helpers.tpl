{{- define "mocache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mocache.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "mocache.labels" -}}
app.kubernetes.io/name: {{ include "mocache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app: mocache
{{- end }}

{{- define "mocache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mocache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: mocache
{{- end }}

{{/*
Common labels
*/}}
{{- define "clotho-control.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "clotho-control.apiImage" -}}
{{ .Values.registry }}/{{ .Values.api.image }}:{{ .Values.api.tag }}
{{- end }}

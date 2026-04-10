{{/*
Common labels
*/}}
{{- define "clotho-system.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "clotho-system.apiImage" -}}
{{ .Values.registry }}/{{ .Values.api.image }}:{{ .Values.api.tag }}
{{- end }}

{{- define "clotho-system.dataProxyImage" -}}
{{ .Values.registry }}/{{ .Values.dataProxy.image }}:{{ .Values.dataProxy.tag }}
{{- end }}

{{- define "clotho-system.uiImage" -}}
{{ .Values.registry }}/{{ .Values.ui.image }}:{{ .Values.ui.tag }}
{{- end }}
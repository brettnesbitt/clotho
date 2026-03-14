{{/*
Common labels
*/}}
{{- define "clotho.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Full image path
*/}}
{{- define "clotho.agentImage" -}}
{{ .Values.registry }}/{{ .Values.agent.image }}:{{ .Values.agent.tag }}
{{- end }}

{{/*
Agent image path
*/}}

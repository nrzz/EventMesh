{{- define "eventmesh.name" -}}
eventmesh
{{- end }}

{{- define "eventmesh.fullname" -}}
{{ .Release.Name }}-eventmesh
{{- end }}

{{- define "eventmesh.labels" -}}
app.kubernetes.io/name: eventmesh
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end }}

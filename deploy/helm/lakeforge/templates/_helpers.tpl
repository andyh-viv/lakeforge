{{- define "lakeforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lakeforge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "lakeforge.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ include "lakeforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "lakeforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lakeforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "lakeforge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "lakeforge.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "lakeforge.secretName" -}}
{{- default (printf "%s-secrets" (include "lakeforge.fullname" .)) .Values.controlPlane.existingSecret -}}
{{- end -}}

{{- define "lakeforge.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{- define "lakeforge.forgeImage" -}}
{{- printf "%s:%s" .Values.forge.image (default .Chart.AppVersion .Values.forge.tag) -}}
{{- end -}}

{{/* Database URL: explicit > embedded postgres > sqlite on the data volume. */}}
{{- define "lakeforge.databaseUrl" -}}
{{- if .Values.database.url -}}
{{- .Values.database.url -}}
{{- else if .Values.database.embeddedPostgres.enabled -}}
{{- printf "postgres://lakeforge:%s@%s-postgres:5432/lakeforge" .Values.database.embeddedPostgres.password (include "lakeforge.fullname" .) -}}
{{- else -}}
sqlite:///var/lib/lakeforge/data/lakeforge.db?mode=rwc
{{- end -}}
{{- end -}}

{{/* Render a map as "k1=v1,k2=v2" (sorted by key). */}}
{{- define "lakeforge.kvList" -}}
{{- $parts := list -}}
{{- range $k, $v := . -}}{{- $parts = append $parts (printf "%s=%s" $k ($v | toString)) -}}{{- end -}}
{{- join "," $parts -}}
{{- end -}}

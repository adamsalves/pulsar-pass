{{/* Shared helpers for the PulsarPass services chart. */}}

{{- define "pulsar-pass.labels" -}}
app.kubernetes.io/name: pulsar-pass
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: pulsar-pass
{{- end -}}

{{- define "pulsar-pass.componentLabels" -}}
{{ include "pulsar-pass.labels" .root }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{- define "pulsar-pass.dsn" -}}
postgres://{{ .root.Values.postgres.user }}:{{ .root.Values.postgres.password }}@{{ .root.Values.postgres.host }}:{{ .root.Values.postgres.port }}/{{ .db }}?sslmode=disable
{{- end -}}

{{/* commonEnv renders the environment shared by every service. */}}
{{- define "pulsar-pass.commonEnv" -}}
- name: APP_ENV
  value: {{ .root.Values.appEnv | quote }}
- name: NATS_URL
  value: {{ .root.Values.natsUrl | quote }}
{{- if .root.Values.otelEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .root.Values.otelEndpoint | quote }}
{{- end }}
{{- end -}}

{{/* serviceEnv renders the per-service environment (contract from each
internal/<svc>/config.go). */}}
{{- define "pulsar-pass.serviceEnv" -}}
{{- $root := .root -}}
{{- $svc := .name -}}
{{- $cfg := index $root.Values.services $svc -}}
{{- if eq $svc "gateway" }}
- name: HTTP_ADDR
  value: ":8080"
- name: BUS_MODE
  value: "nats"
- name: MAX_RESERVATION_QTY
  value: {{ $cfg.maxReservationQty | default "8" | quote }}
{{- /* Same guard as secret-gateway-auth.yaml: an empty authTokens
   skips the Secret AND the reference, otherwise the pod would point
   at a missing Secret (CreateContainerConfigError). */}}
{{- if $cfg.authTokens }}
- name: AUTH_TOKENS
  valueFrom:
    secretKeyRef:
      name: pulsar-gateway-auth
      key: AUTH_TOKENS
{{- end }}
{{- else if eq $svc "core" }}
- name: DATABASE_URL
  value: {{ include "pulsar-pass.dsn" (dict "root" $root "db" "pulsar_core") | quote }}
- name: REDIS_ADDR
  value: {{ $root.Values.redisAddr | quote }}
- name: RESERVATION_TTL
  value: 10m
{{- else if eq $svc "chrono" }}
- name: DATABASE_URL
  value: {{ include "pulsar-pass.dsn" (dict "root" $root "db" "pulsar_core") | quote }}
- name: REDIS_ADDR
  value: {{ $root.Values.redisAddr | quote }}
- name: SWEEP_INTERVAL
  value: {{ $cfg.sweepInterval | default "5s" | quote }}
- name: SWEEP_BATCH
  value: {{ $cfg.sweepBatch | default "100" | quote }}
{{- else if eq $svc "payment" }}
- name: DATABASE_URL
  value: {{ include "pulsar-pass.dsn" (dict "root" $root "db" "pulsar_payment") | quote }}
- name: SIMULATED_FAILURE_RATE
  value: {{ $cfg.simulatedFailureRate | default "0.05" | quote }}
- name: SIMULATED_CHARGE_DELAY
  value: {{ $cfg.simulatedChargeDelay | default "250ms" | quote }}
{{- else if eq $svc "horizon" }}
- name: CORE_DATABASE_URL
  value: {{ include "pulsar-pass.dsn" (dict "root" $root "db" "pulsar_core") | quote }}
- name: PAYMENT_DATABASE_URL
  value: {{ include "pulsar-pass.dsn" (dict "root" $root "db" "pulsar_payment") | quote }}
- name: POLL_INTERVAL
  value: {{ $cfg.pollInterval | default "1s" | quote }}
- name: RELAY_BATCH
  value: {{ $cfg.relayBatch | default "200" | quote }}
{{- end }}
{{- end -}}

{{/* healthPort maps each service to its health/readiness port
(pkg/health defaults, one port per service 9091-9095). */}}
{{- define "pulsar-pass.healthPort" -}}
{{- if eq . "gateway" }}9091
{{- else if eq . "core" }}9092
{{- else if eq . "chrono" }}9093
{{- else if eq . "payment" }}9094
{{- else if eq . "horizon" }}9095
{{- end -}}
{{- end -}}

# ephemeral-pod-metrics-unreliable

**Status:** enforced (alerting convention)

**Symptom:** Grafana panels over click_runner_* Prometheus counters show
"No Data"; a stale table raised no alert.

**Cause:** jobs run as ephemeral CronJob pods. The PodMonitor scrape races the
pod lifetime, and counters reset with every pod. `/health` on :9090 is wired to
no probe. None of this can alert on "job green but table stale".

**Rule:** every production table gets a row in the ClickHouse freshness query
set (`alerting/alerts/click-runner.yaml`, crawlers-data-stale pattern). That —
plus Loki log events — is the alarm. Prometheus/health are debug surfaces.

**Evidence:** `dashboards/click-runner-observability.json` panel note
("replaces the ephemeral-CronJob Prometheus counter that reads No Data between
runs"); no liveness/readiness probes in the CronJob Terraform.

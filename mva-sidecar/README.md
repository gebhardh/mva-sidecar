# Multi-Tenant Mahalanobis Anomaly Detection Sidecar

Skalierbarer Sidecar-Service für multivariate Anomalieerkennung über mehrere
Reaktoren, Standorte oder beliebige überwachte Entitäten. Ein einzelner
Service-Prozess verwaltet alle Tenants parallel in unabhängigen Worker-Threads.

## Neue Features in v2.0

- **Multi-Tenant-Architektur**: ein Prozess für viele überwachte Entitäten
- **Isolation pro Tenant**: eigenes Modell, eigene Feature-Liste, eigene Parameter
- **Globale Defaults + pro-Tenant-Overrides** für Detector-Parameter
- **Tenant-Labels** in allen Metriken für präzises Filtering in Grafana
- **Health-Checks**: automatischer Neustart gestorbener Worker
- **Konfig-Validierung**: `--validate` Flag zum Testen ohne Service-Start
- **Config-Inheritance**: YAML-Anchors für Sensor-Blueprints

## Projekt-Struktur

```
mva-sidecar/
├── mva_sidecar/
│   ├── __init__.py
│   ├── config.py              # Multi-Tenant YAML-Parser
│   ├── vm_client.py           # Thread-safe VictoriaMetrics-Client
│   ├── detector.py            # Mahalanobis-Logic (unverändert)
│   ├── metrics.py             # Prometheus-Metriken mit tenant-Label
│   ├── tenant_worker.py       # Worker pro Tenant (eigener Thread)
│   └── service.py             # Orchestrierung aller Worker
├── examples/
│   ├── config-single-tenant.yaml
│   ├── config-multi-function.yaml
│   ├── vmagent-scrape-config.yaml
│   ├── alert-rules.yaml
│   └── grafana-dashboard.json
├── config.yaml                # Beispiel: 5 Reaktoren an 2 Standorten
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Quick Start

```bash
vi config.yaml

python -m mva_sidecar.service --validate --config config.yaml

docker-compose up -d

curl http://localhost:9100/metrics | grep mva_
```

## Architektur

```
                         ┌─────────────────────────────┐
                         │   MultiTenantSidecar         │
                         │                              │
      ┌──────────────────┼──► TenantWorker(reaktor_a)   │
      │                  │        └─► Detector A        │
      │                  │                              │
VictoriaMetrics ◄────────┼──► TenantWorker(reaktor_b)   │
      │                  │        └─► Detector B        │
      │                  │                              │
      └──────────────────┼──► TenantWorker(reaktor_c)   │
                         │        └─► Detector C        │
                         │                              │
                         │   Shared: VM-Client, Metrics │
                         └──────────────────────────────┘
                                       │
                                       ▼
                              /metrics (Port 9100)
                              alle Tenants mit Label
```

Jeder Tenant läuft in einem eigenen Thread mit:
- Eigenem Detector (eigene Kovarianzmatrix, eigener Schwellwert)
- Eigener Feature-Liste und PromQL-Queries
- Eigenen Labels für alle Metriken
- Optional eigenen Detector-Parametern (Konfidenz, Training-Window)

Geteilt werden nur:
- VM-Client mit Connection-Pool
- Prometheus-Registry
- Logger

## Konfiguration

### Einfach — ein Tenant

```yaml
tenants:
  - id: reaktor_a
    name: "Reaktor A"
    labels:
      function: muenchen
    features:
      - name: temperatur
        query: 'avg(sensor_temperature{instance="A"})'
```

### Mehrere Tenants mit geteilten Defaults

```yaml
detector_defaults:
  confidence: 0.99
  training_window_minutes: 1440

tenants:
  - id: reaktor_a
    name: "Reaktor A"
    labels: {function: muenchen}
    features: [...]

  - id: reaktor_b
    name: "Reaktor B"
    labels: {function: muenchen}
    features: [...]

  - id: reaktor_c_pilot
    name: "Pilotanlage"
    labels: {function: muenchen, line: pilot}
    detector:
      confidence: 0.95
      training_window_minutes: 720
    features: [...]
```

### Tenants deaktivieren (Wartungsmodus)

```yaml
  - id: reaktor_e_wartung
    name: "Reaktor E"
    enabled: false
    features: [...]
```

## Konfigurationshierarchie

```
Globale Defaults (detector_defaults)
         │
         ▼
Pro-Tenant-Override (tenants[].detector)
         │
         ▼
Finale Detector-Config für diesen Tenant
```

Jede Detector-Einstellung kann auf Tenant-Ebene überschrieben werden. Nicht
angegebene Felder erben den Wert aus `detector_defaults`.

## Exportierte Metriken

Alle Metriken haben ein `tenant`-Label für präzises Filtering:

| Metrik | Labels | Beschreibung |
|---|---|---|
| `mva_mahalanobis_d_squared` | tenant | Aktuelle D²-Distanz |
| `mva_is_outlier` | tenant | 1/0 Ausreißer-Flag |
| `mva_severity_level` | tenant | 0-3 Schwere |
| `mva_threshold` | tenant | Chi²-Schwellwert |
| `mva_feature_contribution` | tenant, feature | Feature-Beitrag zu D² |
| `mva_tenant_up` | tenant | 1 wenn Tenant gefittet |
| `mva_tenant_last_success_timestamp_seconds` | tenant | Letzte Prediction |
| `mva_collections_total` | tenant, status | Zyklen-Counter |
| `mva_collection_duration_seconds` | tenant | Histogramm |
| `mva_fit_duration_seconds` | tenant | Fit-Dauer |
| `mva_detector_fits_total` | tenant, status | Fit-Counter |
| `mva_vm_query_errors_total` | tenant | Query-Fehler |
| `mva_service_info` | version, tenants_count | Service-Info |

Zusätzlich im Push-Modus: alle `tenant.labels` aus der Konfiguration werden
als Prometheus-Labels an VictoriaMetrics geschrieben.

## Grafana-Queries

```promql
mva_is_outlier == 1

mva_mahalanobis_d_squared{tenant="reaktor_a"}

avg(mva_mahalanobis_d_squared) by (function)

topk(1, mva_feature_contribution{tenant="reaktor_a"})

sum(mva_tenant_up)

sum(mva_is_outlier) by (function)
```

## Alert-Rules

Siehe `examples/alert-rules.yaml` für produktionsreife Alerts:

- `MahalanobisAnomalyHigh`: schwere Anomalie pro Tenant
- `MahalanobisTenantDown`: Tenant nicht gefittet
- `MahalanobisTenantStale`: keine aktuellen Messungen
- `MahalanobisVMQueryErrors`: VM-Query-Fehler
- `MahalanobisFitFailures`: wiederholte Fit-Fehler

## Skalierung

Ein Sidecar-Prozess kann problemlos 20-50 Tenants parallel verwalten:

- **CPU**: MCD-Fit ist der Hotspot (O(n²) in Samples, alle `refit_interval`
  Minuten). Bei 50 Tenants à 1440 Samples × 5 Features: wenige Sekunden.
- **Speicher**: pro Tenant etwa 10-50 MB (abhängig von Training-Window).
- **Netzwerk**: Connection-Pool mit 50 Connections reicht für alle Tenants.

Für sehr große Deployments (100+ Tenants) empfiehlt sich ein Split in mehrere
Container (ein Container pro Standort oder Produktionslinie).

## Development

```bash
pip install -r requirements.txt

python -m mva_sidecar.service --validate --config config.yaml

python -m mva_sidecar.service --config config.yaml

docker build -t mva-sidecar:2.0 .

docker run -p 9100:9100 -v $(pwd)/config.yaml:/etc/mva-sidecar/config.yaml:ro mva-sidecar:2.0
```

## Migration von v1.0 (Single-Tenant)

Die v1.0-Konfiguration wird in v2.0 als einzelner Tenant formuliert:

```yaml
tenants:
  - id: reaktor_a
    name: "Reaktor A"
    labels:
      service: reaktor_a
      function: muenchen
    features:
      - name: temperatur
        query: 'avg(sensor_temperature{location="reaktor_a"})'
```

Die `extra_labels` aus v1.0 `service:` wandern in `tenants[].labels`. Dashboards
müssen auf `{tenant="reaktor_a"}` umgestellt werden statt Query ohne Label.

## Lizenz

MIT

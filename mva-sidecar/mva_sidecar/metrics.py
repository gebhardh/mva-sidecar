from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry


class SidecarMetrics:
    """
    Prometheus-Metriken für Multi-Tenant-Setup.
    
    Alle Ergebnis-Metriken haben ein 'tenant'-Label, zusätzlich pro Tenant
    beliebige extra_labels aus der Konfiguration. Service-interne Metriken
    (Zyklen, Fehler, Fits) ebenfalls mit tenant-Label für Drilldown.
    """

    def __init__(self, registry: CollectorRegistry, prefix: str = "mva"):
        self.registry = registry
        self.prefix = prefix

        self.d_squared = Gauge(
            f"{prefix}_mahalanobis_d_squared",
            "Mahalanobis-Distanz² des letzten Samples",
            ["tenant"],
            registry=registry,
        )

        self.is_outlier = Gauge(
            f"{prefix}_is_outlier",
            "1 wenn letztes Sample ein Ausreißer ist, sonst 0",
            ["tenant"],
            registry=registry,
        )

        self.severity = Gauge(
            f"{prefix}_severity_level",
            "Schwere-Level: 0=normal, 1=niedrig, 2=mittel, 3=hoch",
            ["tenant"],
            registry=registry,
        )

        self.threshold = Gauge(
            f"{prefix}_threshold",
            "Aktueller Chi-Quadrat-Schwellwert",
            ["tenant"],
            registry=registry,
        )

        self.contribution = Gauge(
            f"{prefix}_feature_contribution",
            "Beitrag eines Features zur D² (Jiang-Dekomposition)",
            ["tenant", "feature"],
            registry=registry,
        )

        self.tenant_up = Gauge(
            f"{prefix}_tenant_up",
            "1 wenn Tenant aktiv und gefittet, sonst 0",
            ["tenant"],
            registry=registry,
        )

        self.tenant_last_success_timestamp = Gauge(
            f"{prefix}_tenant_last_success_timestamp_seconds",
            "Unix-Timestamp der letzten erfolgreichen Prediction",
            ["tenant"],
            registry=registry,
        )

        self.collection_duration = Histogram(
            f"{prefix}_collection_duration_seconds",
            "Dauer eines Collection-Zyklus pro Tenant",
            ["tenant"],
            registry=registry,
        )

        self.fit_duration = Histogram(
            f"{prefix}_fit_duration_seconds",
            "Dauer eines Detector-Fits pro Tenant",
            ["tenant"],
            registry=registry,
        )

        self.collections_total = Counter(
            f"{prefix}_collections_total",
            "Gesamtzahl Collection-Zyklen pro Tenant",
            ["tenant", "status"],
            registry=registry,
        )

        self.vm_query_errors = Counter(
            f"{prefix}_vm_query_errors_total",
            "VictoriaMetrics Query-Fehler pro Tenant",
            ["tenant"],
            registry=registry,
        )

        self.detector_fits = Counter(
            f"{prefix}_detector_fits_total",
            "Gesamtzahl Detector-Fits pro Tenant",
            ["tenant", "status"],
            registry=registry,
        )

        self.service_info = Gauge(
            f"{prefix}_service_info",
            "Service-Information (immer 1)",
            ["version", "tenants_count"],
            registry=registry,
        )


SEVERITY_MAP = {"normal": 0, "niedrig": 1, "mittel": 2, "hoch": 3}

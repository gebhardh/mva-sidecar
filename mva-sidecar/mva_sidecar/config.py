from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import yaml
import os
import copy


@dataclass
class FeatureConfig:
    name: str
    query: str


@dataclass
class VMConfig:
    read_url: str = "http://victoriametrics:8428"
    write_url: str = "http://victoriametrics:8428/api/v1/write"
    timeout_seconds: int = 30
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class DetectorConfig:
    confidence: float = 0.99
    use_robust: bool = True
    support_fraction: float = 0.75
    variance_threshold: float = 1e-8
    training_window_minutes: int = 1440
    refit_interval_minutes: int = 60
    min_training_samples: int = 100


@dataclass
class ServiceConfig:
    collection_interval_seconds: int = 60
    metrics_port: int = 9100
    log_level: str = "INFO"
    output_prefix: str = "mva"
    push_mode: str = "exporter"
    worker_threads: int = 0


@dataclass
class TenantConfig:
    """
    Konfiguration für einen einzelnen Tenant (z.B. einen Reaktor).
    
    Jeder Tenant hat eigene Features, Labels und optional überschriebene
    Detector-Parameter. Die globalen Defaults kommen von der Service-Konfiguration.
    """
    id: str
    name: str
    labels: Dict[str, str]
    features: List[FeatureConfig]
    detector: DetectorConfig = field(default_factory=DetectorConfig)
    enabled: bool = True


@dataclass
class Config:
    """
    Top-Level-Konfiguration. Enthält globale Einstellungen und eine Liste
    von Tenants. Jeder Tenant wird von einem eigenen Worker-Thread verarbeitet.
    """
    tenants: List[TenantConfig]
    vm: VMConfig = field(default_factory=VMConfig)
    detector_defaults: DetectorConfig = field(default_factory=DetectorConfig)
    service: ServiceConfig = field(default_factory=ServiceConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        vm = VMConfig(**raw.get("vm", {}))
        detector_defaults = DetectorConfig(**raw.get("detector_defaults", {}))
        service = ServiceConfig(**raw.get("service", {}))

        tenants_raw = raw.get("tenants", [])
        if not tenants_raw:
            raise ValueError("Keine Tenants konfiguriert")

        tenants = []
        seen_ids = set()
        for t_raw in tenants_raw:
            tenant = _parse_tenant(t_raw, detector_defaults)
            if tenant.id in seen_ids:
                raise ValueError(f"Doppelte Tenant-ID: {tenant.id}")
            seen_ids.add(tenant.id)
            tenants.append(tenant)

        _expand_env_vars(vm)

        return cls(
            tenants=tenants,
            vm=vm,
            detector_defaults=detector_defaults,
            service=service,
        )


def _parse_tenant(raw: Dict[str, Any], defaults: DetectorConfig) -> TenantConfig:
    """Einen Tenant aus Dict parsen, mit Defaults aus globaler Config."""
    if "id" not in raw:
        raise ValueError("Tenant ohne 'id' konfiguriert")
    if "features" not in raw or not raw["features"]:
        raise ValueError(f"Tenant {raw['id']}: keine Features konfiguriert")

    features = [FeatureConfig(**f) for f in raw["features"]]

    detector_override = raw.get("detector", {})
    detector = copy.deepcopy(defaults)
    for key, val in detector_override.items():
        if hasattr(detector, key):
            setattr(detector, key, val)

    return TenantConfig(
        id=raw["id"],
        name=raw.get("name", raw["id"]),
        labels=raw.get("labels", {}),
        features=features,
        detector=detector,
        enabled=raw.get("enabled", True),
    )


def _expand_env_vars(vm: VMConfig):
    """Ersetze ${VAR_NAME} Syntax durch Umgebungsvariablen."""
    for attr in ["username", "password", "read_url", "write_url"]:
        val = getattr(vm, attr)
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            env_var = val[2:-1]
            setattr(vm, attr, os.environ.get(env_var))

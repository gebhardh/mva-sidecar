import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import VMConfig

log = logging.getLogger(__name__)


@dataclass
class Sample:
    timestamp: datetime
    value: float


class VictoriaMetricsClient:
    """
    Thread-safer Client für Query- und Write-Operationen gegen VictoriaMetrics.
    requests.Session ist thread-safe bei separaten Connection-Pools pro Thread.
    Wir nutzen einen Adapter mit großem Pool.
    """

    def __init__(self, config: VMConfig):
        self.config = config
        self.session = requests.Session()

        retry = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=retry,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if config.username and config.password:
            self.session.auth = (config.username, config.password)

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step: str = "60s",
    ) -> List[Sample]:
        """Führe eine PromQL-Range-Query aus und liefere die Samples zurück."""
        url = f"{self.config.read_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "step": step,
        }

        try:
            resp = self.session.get(url, params=params, timeout=self.config.timeout_seconds)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.error(f"Query fehlgeschlagen: {query[:60]}... - {e}")
            return []

        if data.get("status") != "success":
            log.warning(f"Query nicht erfolgreich: {data.get('error', 'Unbekannt')}")
            return []

        result = data["data"]["result"]
        if not result:
            return []

        series = result[0]
        samples = []
        for ts, val in series["values"]:
            try:
                samples.append(Sample(
                    timestamp=datetime.fromtimestamp(float(ts)),
                    value=float(val),
                ))
            except (ValueError, TypeError):
                continue
        return samples

    def query_instant(self, query: str) -> Optional[float]:
        """Aktueller Wert für eine PromQL-Query."""
        url = f"{self.config.read_url}/api/v1/query"
        params = {"query": query, "time": int(time.time())}

        try:
            resp = self.session.get(url, params=params, timeout=self.config.timeout_seconds)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.error(f"Instant query fehlgeschlagen: {e}")
            return None

        result = data.get("data", {}).get("result", [])
        if not result:
            return None

        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError):
            return None

    def write_prometheus_format(self, metrics: List[tuple]):
        """
        Schreibe Metriken im Prometheus Exposition Format via /api/v1/import/prometheus.
        Jede Metrik ist ein Tuple: (name, labels_dict, value, timestamp_ms).
        """
        url = f"{self.config.read_url}/api/v1/import/prometheus"

        lines = []
        for name, labels, value, ts_ms in metrics:
            if value != value:
                continue
            label_str = ",".join(f'{k}="{_escape_label(v)}"' for k, v in labels.items())
            lines.append(f'{name}{{{label_str}}} {value} {ts_ms}')

        if not lines:
            return

        payload = "\n".join(lines) + "\n"

        try:
            resp = self.session.post(url, data=payload, timeout=self.config.timeout_seconds)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"Prometheus import fehlgeschlagen: {e}")


def _escape_label(value: str) -> str:
    """Escape Label-Werte für Line Protocol und Prometheus Format."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict
import numpy as np

from .config import TenantConfig, ServiceConfig
from .vm_client import VictoriaMetricsClient
from .detector import RobustMahalanobisDetector
from .metrics import SidecarMetrics, SEVERITY_MAP

log = logging.getLogger(__name__)


class TenantWorker:
    """
    Worker für einen einzelnen Tenant. Läuft in eigenem Thread.
    Hat seinen eigenen Detector-Zustand, teilt aber VM-Client und Metrics-Registry.
    """

    def __init__(
        self,
        tenant: TenantConfig,
        service_config: ServiceConfig,
        vm_client: VictoriaMetricsClient,
        metrics: SidecarMetrics,
        stop_event: threading.Event,
    ):
        self.tenant = tenant
        self.service_config = service_config
        self.vm_client = vm_client
        self.metrics = metrics
        self.stop_event = stop_event

        feature_names = [f.name for f in tenant.features]
        self.detector = RobustMahalanobisDetector(
            tenant.detector, feature_names, tenant_id=tenant.id
        )

        self.last_fit_time: Optional[datetime] = None
        self.thread: Optional[threading.Thread] = None

    def start(self):
        """Starte den Worker-Thread."""
        self.thread = threading.Thread(
            target=self._run,
            name=f"tenant-{self.tenant.id}",
            daemon=True,
        )
        self.thread.start()

    def join(self, timeout: Optional[float] = None):
        """Warte auf Thread-Ende."""
        if self.thread:
            self.thread.join(timeout=timeout)

    def _run(self):
        """Haupt-Loop für diesen Tenant."""
        log.info(f"[{self.tenant.id}] Worker gestartet ({self.tenant.name})")

        self.metrics.tenant_up.labels(tenant=self.tenant.id).set(0)

        startup_jitter = hash(self.tenant.id) % 10
        if startup_jitter > 0:
            log.debug(f"[{self.tenant.id}] Startup-Jitter: {startup_jitter}s")
            if self.stop_event.wait(startup_jitter):
                return

        log.info(f"[{self.tenant.id}] Initial fit...")
        self._fit_detector()

        interval = self.service_config.collection_interval_seconds
        while not self.stop_event.is_set():
            start = time.time()
            try:
                self._collection_cycle()
                self.metrics.collections_total.labels(
                    tenant=self.tenant.id, status="success"
                ).inc()
            except Exception as e:
                log.exception(f"[{self.tenant.id}] Fehler im Collection-Zyklus: {e}")
                self.metrics.collections_total.labels(
                    tenant=self.tenant.id, status="error"
                ).inc()
            finally:
                duration = time.time() - start
                self.metrics.collection_duration.labels(tenant=self.tenant.id).observe(duration)

            if self._should_refit():
                log.info(f"[{self.tenant.id}] Refit erforderlich...")
                self._fit_detector()

            elapsed = time.time() - start
            sleep_time = max(0, interval - elapsed)
            if self.stop_event.wait(sleep_time):
                break

        self.metrics.tenant_up.labels(tenant=self.tenant.id).set(0)
        log.info(f"[{self.tenant.id}] Worker beendet")

    def _collection_cycle(self):
        """Ein Collection-Zyklus: aktuelle Werte abfragen und klassifizieren."""
        current_sample = self._fetch_current_sample()

        if current_sample is None:
            return

        result = self.detector.predict(current_sample)

        if result is None:
            return

        labels = {"tenant": self.tenant.id}
        self.metrics.d_squared.labels(**labels).set(result.d_squared)
        self.metrics.is_outlier.labels(**labels).set(1 if result.is_outlier else 0)
        self.metrics.severity.labels(**labels).set(SEVERITY_MAP[result.severity])
        self.metrics.threshold.labels(**labels).set(result.threshold)
        self.metrics.tenant_last_success_timestamp.labels(**labels).set(time.time())

        for feat_name, contrib in result.contributions.items():
            self.metrics.contribution.labels(
                tenant=self.tenant.id, feature=feat_name
            ).set(contrib)

        if self.service_config.push_mode == "push":
            self._push_to_vm(result)

        if result.is_outlier:
            top_feat = max(result.contributions.items(), key=lambda x: abs(x[1]))
            log.warning(
                f"[{self.tenant.id}] Anomalie: D²={result.d_squared:.2f} "
                f"({result.severity}), Haupttreiber: {top_feat[0]} ({top_feat[1]:+.2f})"
            )
        else:
            log.debug(f"[{self.tenant.id}] Normal: D²={result.d_squared:.2f}")

    def _fetch_current_sample(self) -> Optional[np.ndarray]:
        """Lies für jedes Feature den aktuellen Wert aus VictoriaMetrics."""
        values = []
        for feature in self.tenant.features:
            val = self.vm_client.query_instant(feature.query)
            if val is None:
                self.metrics.vm_query_errors.labels(tenant=self.tenant.id).inc()
                values.append(np.nan)
            else:
                values.append(val)
        return np.array(values)

    def _fit_detector(self) -> bool:
        """Hole Trainingsdaten aus VM und fitte den Detektor."""
        start = time.time()
        try:
            end = datetime.now()
            training_window = timedelta(minutes=self.tenant.detector.training_window_minutes)
            begin = end - training_window

            log.info(f"[{self.tenant.id}] Hole Trainingsdaten von {begin} bis {end}")

            samples_per_feature = []
            for feature in self.tenant.features:
                samples = self.vm_client.query_range(
                    feature.query, begin, end,
                    step=f"{self.service_config.collection_interval_seconds}s",
                )
                if not samples:
                    log.error(
                        f"[{self.tenant.id}] Keine Trainingsdaten für Feature '{feature.name}'"
                    )
                    self.metrics.detector_fits.labels(
                        tenant=self.tenant.id, status="error_no_data"
                    ).inc()
                    return False
                samples_per_feature.append(samples)

            X = self._align_samples(samples_per_feature)
            if X is None or X.shape[0] < self.tenant.detector.min_training_samples:
                n = X.shape[0] if X is not None else 0
                log.error(f"[{self.tenant.id}] Zu wenige alignierbare Samples: {n}")
                self.metrics.detector_fits.labels(
                    tenant=self.tenant.id, status="error_too_few"
                ).inc()
                return False

            log.info(
                f"[{self.tenant.id}] Fitting auf {X.shape[0]} Samples × {X.shape[1]} Features"
            )
            success = self.detector.fit(X)

            if success:
                self.last_fit_time = datetime.now()
                self.metrics.detector_fits.labels(
                    tenant=self.tenant.id, status="success"
                ).inc()
                self.metrics.tenant_up.labels(tenant=self.tenant.id).set(1)
            else:
                self.metrics.detector_fits.labels(
                    tenant=self.tenant.id, status="error_fit"
                ).inc()

            return success

        finally:
            self.metrics.fit_duration.labels(tenant=self.tenant.id).observe(time.time() - start)

    def _align_samples(self, samples_per_feature: list) -> Optional[np.ndarray]:
        """Aligniere Samples aller Features auf gemeinsame Zeitstempel."""
        if not samples_per_feature:
            return None

        timestamp_sets = [
            {s.timestamp.replace(microsecond=0) for s in samples}
            for samples in samples_per_feature
        ]
        common = sorted(set.intersection(*timestamp_sets))

        if not common:
            log.error(f"[{self.tenant.id}] Keine gemeinsamen Zeitstempel")
            return None

        X = np.full((len(common), len(samples_per_feature)), np.nan)
        for j, samples in enumerate(samples_per_feature):
            by_ts = {s.timestamp.replace(microsecond=0): s.value for s in samples}
            for i, ts in enumerate(common):
                if ts in by_ts:
                    X[i, j] = by_ts[ts]

        return X

    def _push_to_vm(self, result):
        """Push-Modus: schreibe Ergebnisse direkt an VM mit Tenant-Labels."""
        ts_ms = int(time.time() * 1000)
        base_labels = {"tenant": self.tenant.id, **self.tenant.labels}
        prefix = self.service_config.output_prefix

        metrics = [
            (f"{prefix}_mahalanobis_d_squared", base_labels, result.d_squared, ts_ms),
            (f"{prefix}_is_outlier", base_labels, 1 if result.is_outlier else 0, ts_ms),
            (f"{prefix}_severity_level", base_labels, SEVERITY_MAP[result.severity], ts_ms),
            (f"{prefix}_threshold", base_labels, result.threshold, ts_ms),
        ]

        for feat_name, contrib in result.contributions.items():
            feat_labels = {**base_labels, "feature": feat_name}
            metrics.append((f"{prefix}_feature_contribution", feat_labels, contrib, ts_ms))

        self.vm_client.write_prometheus_format(metrics)

    def _should_refit(self) -> bool:
        if self.last_fit_time is None:
            return True
        elapsed = (datetime.now() - self.last_fit_time).total_seconds()
        return elapsed >= self.tenant.detector.refit_interval_minutes * 60

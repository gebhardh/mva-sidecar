import logging
import signal
import sys
import threading
from typing import List
from prometheus_client import CollectorRegistry, start_http_server

from . import __version__
from .config import Config
from .vm_client import VictoriaMetricsClient
from .metrics import SidecarMetrics
from .tenant_worker import TenantWorker

log = logging.getLogger(__name__)


class MultiTenantSidecar:
    """
    Multi-Tenant-Service: verwaltet mehrere TenantWorker in parallelen Threads.
    Teilt VM-Client (mit Connection-Pool) und Metrics-Registry.
    """

    def __init__(self, config: Config):
        self.config = config
        self.vm_client = VictoriaMetricsClient(config.vm)

        self.registry = CollectorRegistry()
        self.metrics = SidecarMetrics(self.registry, config.service.output_prefix)

        enabled_tenants = [t for t in config.tenants if t.enabled]
        disabled_count = len(config.tenants) - len(enabled_tenants)

        if disabled_count > 0:
            log.info(f"{disabled_count} Tenants deaktiviert - werden übersprungen")

        self.metrics.service_info.labels(
            version=__version__,
            tenants_count=str(len(enabled_tenants)),
        ).set(1)

        self.stop_event = threading.Event()

        self.workers: List[TenantWorker] = [
            TenantWorker(
                tenant=tenant,
                service_config=config.service,
                vm_client=self.vm_client,
                metrics=self.metrics,
                stop_event=self.stop_event,
            )
            for tenant in enabled_tenants
        ]

    def run(self):
        """Starte Metrics-Server, alle Worker-Threads und warte auf Shutdown."""
        log.info(
            f"Starte Multi-Tenant Sidecar v{__version__} mit {len(self.workers)} Tenants "
            f"auf Port {self.config.service.metrics_port}"
        )
        start_http_server(self.config.service.metrics_port, registry=self.registry)

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        log.info("Starte Tenant-Worker:")
        for worker in self.workers:
            log.info(f"  - {worker.tenant.id} ({worker.tenant.name})")
            worker.start()

        log.info("Alle Worker gestartet - warte auf Shutdown-Signal")

        try:
            while not self.stop_event.is_set():
                self.stop_event.wait(timeout=1.0)
                self._check_worker_health()
        except KeyboardInterrupt:
            self._handle_shutdown(signal.SIGINT, None)

        log.info("Warte auf Worker-Threads...")
        shutdown_timeout = self.config.service.collection_interval_seconds + 30
        for worker in self.workers:
            worker.join(timeout=shutdown_timeout)
            if worker.thread and worker.thread.is_alive():
                log.warning(f"Worker {worker.tenant.id} hat nicht rechtzeitig beendet")

        log.info("Service beendet")

    def _check_worker_health(self):
        """Prüfe ob alle Worker-Threads noch laufen."""
        for worker in self.workers:
            if worker.thread and not worker.thread.is_alive() and not self.stop_event.is_set():
                log.error(f"Worker {worker.tenant.id} ist gestorben - versuche Neustart")
                worker.start()

    def _handle_shutdown(self, signum, frame):
        log.info(f"Signal {signum} erhalten - fahre herunter...")
        self.stop_event.set()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-Tenant Mahalanobis Sidecar")
    parser.add_argument("--config", default="/etc/mva-sidecar/config.yaml")
    parser.add_argument("--validate", action="store_true",
                       help="Nur Konfiguration validieren und beenden")
    args = parser.parse_args()

    try:
        config = Config.from_yaml(args.config)
    except Exception as e:
        print(f"Konfigurationsfehler: {e}", file=sys.stderr)
        sys.exit(1)

    if args.validate:
        print(f"Konfiguration ok: {len(config.tenants)} Tenants konfiguriert")
        for t in config.tenants:
            status = "aktiv" if t.enabled else "deaktiviert"
            print(f"  - {t.id} ({t.name}): {len(t.features)} Features [{status}]")
        sys.exit(0)

    logging.basicConfig(
        level=getattr(logging, config.service.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    service = MultiTenantSidecar(config)
    service.run()


if __name__ == "__main__":
    main()

"""
Multi-Tenant Mahalanobis Anomaly Detection Sidecar für VictoriaMetrics.

Periodische multivariate Anomalieerkennung mit robustem MCD-Schätzer,
Feature-Attribution und Prometheus-Metriken-Export. Unterstützt die
parallele Überwachung mehrerer Tenants (z.B. Reaktoren, Standorte) in
einem einzigen Service-Prozess.
"""
__version__ = "2.0.0"

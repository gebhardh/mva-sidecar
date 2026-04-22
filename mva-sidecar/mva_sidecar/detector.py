import logging
from typing import List, Optional, Dict
from dataclasses import dataclass
import numpy as np
from scipy.stats import chi2
from sklearn.covariance import MinCovDet

from .config import DetectorConfig

log = logging.getLogger(__name__)


@dataclass
class AnomalyResult:
    d_squared: float
    is_outlier: bool
    severity: str
    contributions: Dict[str, float]
    threshold: float
    n_features: int


class RobustMahalanobisDetector:
    """
    Robuste Mahalanobis-Ausreißererkennung mit MCD-Schätzer,
    automatischer Entfernung konstanter Features und Pseudo-Inverse-Fallback.
    
    Jede Instanz gehört zu genau einem Tenant und wird nur von dessen
    Worker-Thread zugegriffen - daher ist keine eigene Synchronisation nötig.
    """

    def __init__(self, config: DetectorConfig, feature_names: List[str], tenant_id: str = ""):
        self.config = config
        self.feature_names = feature_names
        self.tenant_id = tenant_id
        self.mean_: Optional[np.ndarray] = None
        self.cov_inv_: Optional[np.ndarray] = None
        self.valid_features_: Optional[List[str]] = None
        self.valid_indices_: Optional[np.ndarray] = None
        self.threshold_: Optional[float] = None
        self.fit_count_: int = 0

    def fit(self, X: np.ndarray) -> bool:
        """
        Lerne Mittelwert und inverse Kovarianz aus Trainingsdaten.
        Returns True wenn Fit erfolgreich war.
        """
        prefix = f"[{self.tenant_id}] " if self.tenant_id else ""

        if X.shape[0] < self.config.min_training_samples:
            log.warning(
                f"{prefix}Zu wenige Trainings-Samples: "
                f"{X.shape[0]} < {self.config.min_training_samples}"
            )
            return False

        valid_mask = ~np.isnan(X).any(axis=1)
        X_clean = X[valid_mask]

        if X_clean.shape[0] < self.config.min_training_samples:
            log.warning(f"{prefix}Nach NaN-Entfernung zu wenige Samples: {X_clean.shape[0]}")
            return False

        variances = np.var(X_clean, axis=0)
        valid_features_mask = variances > self.config.variance_threshold
        n_removed = (~valid_features_mask).sum()

        if n_removed > 0:
            removed = [self.feature_names[i] for i, v in enumerate(valid_features_mask) if not v]
            log.info(f"{prefix}Konstante Features entfernt: {removed}")

        self.valid_indices_ = np.where(valid_features_mask)[0]
        self.valid_features_ = [self.feature_names[i] for i in self.valid_indices_]
        X_clean = X_clean[:, valid_features_mask]

        if X_clean.shape[1] < 2:
            log.error(f"{prefix}Zu wenige valide Features nach Entfernung konstanter Features")
            return False

        if self.config.use_robust:
            try:
                estimator = MinCovDet(
                    support_fraction=self.config.support_fraction,
                    random_state=42,
                ).fit(X_clean)
                self.mean_ = estimator.location_
                cov = estimator.covariance_
                log.info(f"{prefix}MCD-Schätzer erfolgreich")
            except Exception as e:
                log.warning(f"{prefix}MCD fehlgeschlagen ({e}), fallback auf klassische Kovarianz")
                self.mean_ = np.mean(X_clean, axis=0)
                cov = np.cov(X_clean, rowvar=False)
        else:
            self.mean_ = np.mean(X_clean, axis=0)
            cov = np.cov(X_clean, rowvar=False)

        try:
            self.cov_inv_ = np.linalg.inv(cov)
        except np.linalg.LinAlgError:
            log.warning(f"{prefix}Kovarianzmatrix singulär - verwende Pseudo-Inverse")
            self.cov_inv_ = np.linalg.pinv(cov)

        n_features = X_clean.shape[1]
        self.threshold_ = chi2.ppf(self.config.confidence, df=n_features)
        self.fit_count_ += 1

        log.info(
            f"{prefix}Detektor gefittet: {n_features} Features, "
            f"{X_clean.shape[0]} Samples, Threshold={self.threshold_:.2f}"
        )
        return True

    def predict(self, x: np.ndarray) -> Optional[AnomalyResult]:
        """Klassifiziere einen einzelnen Sample."""
        if self.mean_ is None or self.cov_inv_ is None:
            return None

        x_valid = x[self.valid_indices_]

        if np.isnan(x_valid).any():
            return None

        d = x_valid - self.mean_
        cov_inv_d = self.cov_inv_ @ d
        d_squared = float(d @ cov_inv_d)
        contributions_valid = d * cov_inv_d

        contributions = {name: 0.0 for name in self.feature_names}
        for idx, feat_idx in enumerate(self.valid_indices_):
            contributions[self.feature_names[feat_idx]] = float(contributions_valid[idx])

        if d_squared > self.threshold_ * 2.6:
            severity = "hoch"
        elif d_squared > self.threshold_ * 1.7:
            severity = "mittel"
        elif d_squared > self.threshold_:
            severity = "niedrig"
        else:
            severity = "normal"

        return AnomalyResult(
            d_squared=d_squared,
            is_outlier=d_squared > self.threshold_,
            severity=severity,
            contributions=contributions,
            threshold=float(self.threshold_),
            n_features=len(self.valid_indices_),
        )

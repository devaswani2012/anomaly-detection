#!/usr/bin/env python3
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from typing import Optional

from logger import logger


class AnomalyDetector:

    def __init__(self, z_threshold: float = 3.0, contamination: float = 0.05):
        self.z_threshold = z_threshold
        self.contamination = contamination

    def zscore_flag(
        self,
        values: pd.Series,
        mean: float,
        std: float
    ) -> pd.Series:
        try:
            if std == 0:
                logger.info("Standard deviation is 0, returning zero z-scores")
                return pd.Series([0.0] * len(values))
            z_scores = (values - mean).abs() / std
            logger.info("Z-score calculation completed successfully")
            return z_scores
        except Exception as e:
            logger.error(f"Error in zscore_flag: {str(e)}")
            raise

    def isolation_forest_flag(self, df: pd.DataFrame, numeric_cols: list[str]) -> np.ndarray:
        try:
            model = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100
            )
            X = df[numeric_cols].fillna(df[numeric_cols].median())
            model.fit(X)

            labels = model.predict(X)
            scores = model.decision_function(X)

            logger.info("Isolation Forest completed successfully")
            return labels, scores
        except Exception as e:
            logger.error(f"Error in isolation_forest_flag: {str(e)}")
            raise

    def run(
        self,
        df: pd.DataFrame,
        numeric_cols: list[str],
        baseline: dict,
        method: str = "both"
    ) -> pd.DataFrame:
        try:
            result = df.copy()

            if method in ("zscore", "both"):
                for col in numeric_cols:
                    stats = baseline.get(col)
                    if stats and stats["count"] >= 30:
                        z_scores = self.zscore_flag(df[col], stats["mean"], stats["std"])
                        result[f"{col}_zscore"] = z_scores.round(4)
                        result[f"{col}_zscore_flag"] = z_scores > self.z_threshold
                        logger.info(f"Z-score flags created for column {col}")
                    else:
                        result[f"{col}_zscore"] = None
                        result[f"{col}_zscore_flag"] = None
                        logger.info(f"Insufficient baseline history for column {col}")

            if method in ("isolation", "both"):
                labels, scores = self.isolation_forest_flag(df, numeric_cols)
                result["if_label"] = labels
                result["if_score"] = scores.round(4)
                result["if_flag"] = labels == -1
                logger.info("Isolation Forest fields added to result")

            if method == "both":
                zscore_flags = [
                    result[f"{col}_zscore_flag"]
                    for col in numeric_cols
                    if f"{col}_zscore_flag" in result.columns
                    and result[f"{col}_zscore_flag"].notna().any()
                ]
                if zscore_flags:
                    any_zscore = pd.concat(zscore_flags, axis=1).any(axis=1)
                    result["anomaly"] = any_zscore | result["if_flag"]
                else:
                    result["anomaly"] = result["if_flag"]
                logger.info("Final anomaly flag created")

            return result
        except Exception as e:
            logger.error(f"Error in detector run: {str(e)}")
            raise

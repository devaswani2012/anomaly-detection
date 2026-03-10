#!/usr/bin/env python3
import json
import math
import boto3
from datetime import datetime
from typing import Optional

from logger import logger

s3 = boto3.client("s3")


class BaselineManager:

    def __init__(
        self,
        bucket: str,
        baseline_key: str = "state/baseline.json",
        log_key: str = "state/app.log"
    ):
        self.bucket = bucket
        self.baseline_key = baseline_key
        self.log_key = log_key

    def load(self) -> dict:
        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.baseline_key)
            baseline = json.loads(response["Body"].read())
            logger.info(f"Baseline loaded from s3://{self.bucket}/{self.baseline_key}")
            return baseline
        except s3.exceptions.NoSuchKey:
            logger.info(f"No baseline found at s3://{self.bucket}/{self.baseline_key}, starting empty")
            return {}
        except Exception as e:
            logger.error(f"Error loading baseline: {str(e)}")
            raise

    def save(self, baseline: dict):
        try:
            baseline["last_updated"] = datetime.utcnow().isoformat()

            s3.put_object(
                Bucket=self.bucket,
                Key=self.baseline_key,
                Body=json.dumps(baseline, indent=2),
                ContentType="application/json"
            )
            logger.info(f"Baseline saved to s3://{self.bucket}/{self.baseline_key}")

            with open("app.log", "rb") as log_file:
                s3.put_object(
                    Bucket=self.bucket,
                    Key=self.log_key,
                    Body=log_file.read(),
                    ContentType="text/plain"
                )
            logger.info(f"Log file synced to s3://{self.bucket}/{self.log_key}")
        except FileNotFoundError:
            logger.error("app.log not found while attempting S3 log sync")
            raise
        except Exception as e:
            logger.error(f"Error saving baseline or syncing log: {str(e)}")
            raise

    def update(self, baseline: dict, channel: str, new_values: list[float]) -> dict:
        try:
            if channel not in baseline:
                baseline[channel] = {"count": 0, "mean": 0.0, "M2": 0.0}

            state = baseline[channel]

            for value in new_values:
                state["count"] += 1
                delta = value - state["mean"]
                state["mean"] += delta / state["count"]
                delta2 = value - state["mean"]
                state["M2"] += delta * delta2

            if state["count"] >= 2:
                variance = state["M2"] / state["count"]
                state["std"] = math.sqrt(variance)
            else:
                state["std"] = 0.0

            baseline[channel] = state
            logger.info(f"Baseline updated for channel {channel} with {len(new_values)} new values")
            return baseline
        except Exception as e:
            logger.error(f"Error updating baseline for channel {channel}: {str(e)}")
            raise

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        try:
            stats = baseline.get(channel)
            logger.info(f"Retrieved stats for channel {channel}")
            return stats
        except Exception as e:
            logger.error(f"Error getting stats for channel {channel}: {str(e)}")
            raise

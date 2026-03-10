#!/usr/bin/env python3
import json
import io
import boto3
import pandas as pd
from datetime import datetime

from baseline import BaselineManager
from detector import AnomalyDetector
from logger import logger

s3 = boto3.client("s3")

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]

def process_file(bucket: str, key: str):
    logger.info(f"Processing started for s3://{bucket}/{key}")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        logger.info(f"Loaded {len(df)} rows from {key} with columns {list(df.columns)}")
    except Exception as e:
        logger.error(f"Failed to download or read file {key}: {str(e)}")
        raise

    try:
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()
        logger.info("Baseline loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load baseline: {str(e)}")
        raise

    try:
        for col in NUMERIC_COLS:
            if col in df.columns:
                clean_values = df[col].dropna().tolist()
                if clean_values:
                    baseline = baseline_mgr.update(baseline, col, clean_values)
                    logger.info(f"Updated baseline for column {col} with {len(clean_values)} values")
    except Exception as e:
        logger.error(f"Failed while updating baseline: {str(e)}")
        raise

    try:
        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, NUMERIC_COLS, baseline, method="both")
        logger.info("Anomaly detection completed successfully")
    except Exception as e:
        logger.error(f"Failed during anomaly detection: {str(e)}")
        raise

    try:
        output_key = key.replace("raw/", "processed/")
        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        logger.info(f"Processed CSV written to s3://{bucket}/{output_key}")
    except Exception as e:
        logger.error(f"Failed to write processed file: {str(e)}")
        raise

    try:
        baseline_mgr.save(baseline)
        logger.info("Baseline saved successfully")
    except Exception as e:
        logger.error(f"Failed to save baseline: {str(e)}")
        raise

    anomaly_count = int(scored_df["anomaly"].sum()) if "anomaly" in scored_df else 0
    summary = {
        "source_key": key,
        "output_key": output_key,
        "processed_at": datetime.utcnow().isoformat(),
        "total_rows": len(df),
        "anomaly_count": anomaly_count,
        "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
        "baseline_observation_counts": {
            col: baseline.get(col, {}).get("count", 0) for col in NUMERIC_COLS
        }
    }

    try:
        summary_key = output_key.replace(".csv", "_summary.json")
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Summary JSON written to s3://{bucket}/{summary_key}")
    except Exception as e:
        logger.error(f"Failed to write summary file: {str(e)}")
        raise

    logger.info(f"Processing finished for {key}: {anomaly_count}/{len(df)} anomalies flagged")
    return summary

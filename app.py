import os
import json
import io
import boto3
import urllib.request
import pandas as pd
from fastapi import FastAPI, Request, BackgroundTasks

from processor import process_file
from baseline import BaselineManager
from logger import logger

app = FastAPI()

logger.info("Anomaly Detection API started")

BUCKET_NAME = os.environ["BUCKET_NAME"]

s3 = boto3.client("s3")


@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"message": "Anomaly detection service running"}


@app.get("/health")
def health():
    logger.info("Health endpoint called")
    return {"status": "ok"}


@app.get("/baseline/current")
def current_baseline():
    logger.info("Baseline requested")
    try:
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()
        return baseline
    except Exception as e:
        logger.error(f"Error loading current baseline: {str(e)}")
        return {"error": "Failed to load baseline"}


@app.get("/anomalies/summary")
def anomaly_summary():
    logger.info("Anomaly summary requested")
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="processed/")
        contents = response.get("Contents", [])

        summary_keys = [
            obj["Key"] for obj in contents
            if obj["Key"].endswith("_summary.json")
        ]

        total_rows = 0
        total_anomalies = 0
        files_processed = 0

        for key in summary_keys:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            summary = json.loads(obj["Body"].read())

            total_rows += summary.get("total_rows", 0)
            total_anomalies += summary.get("anomaly_count", 0)
            files_processed += 1

        overall_rate = round(total_anomalies / total_rows, 4) if total_rows > 0 else 0

        return {
            "files_processed": files_processed,
            "total_rows": total_rows,
            "total_anomalies": total_anomalies,
            "overall_anomaly_rate": overall_rate
        }
    except Exception as e:
        logger.error(f"Error generating anomaly summary: {str(e)}")
        return {"error": "Failed to generate summary"}


@app.get("/anomalies/recent")
def anomalies_recent(limit: int = 50):
    logger.info(f"Recent anomalies requested with limit={limit}")
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="processed/")
        contents = response.get("Contents", [])

        csv_files = [
            obj["Key"] for obj in contents
            if obj["Key"].endswith(".csv")
        ]

        recent_files = sorted(csv_files, reverse=True)[:10]

        rows = []
        for key in recent_files:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))
            if "anomaly" in df.columns:
                flagged = df[df["anomaly"] == True].copy()
                if not flagged.empty:
                    flagged["source_file"] = key
                    rows.append(flagged)

        if not rows:
            return {"anomalies": [], "count": 0}

        result = pd.concat(rows, ignore_index=True).head(limit)
        return {
            "anomalies": result.to_dict(orient="records"),
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error getting recent anomalies: {str(e)}")
        return {"error": "Failed to load recent anomalies"}


@app.post("/notify")
async def notify(request: Request, background_tasks: BackgroundTasks):
    logger.info("SNS message received")

    try:
        body = await request.json()
        message_type = body.get("Type")

        if message_type == "SubscriptionConfirmation":
            subscribe_url = body.get("SubscribeURL")
            if subscribe_url:
                urllib.request.urlopen(subscribe_url)
                logger.info("SNS subscription confirmed")
                return {"status": "subscription confirmed"}
            logger.error("SubscriptionConfirmation received without SubscribeURL")
            return {"error": "missing SubscribeURL"}

        if message_type == "Notification":
            message = json.loads(body["Message"])
            record = message["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            logger.info(f"Queueing processing for s3://{bucket}/{key}")
            background_tasks.add_task(process_file, bucket, key)

            return {"status": "accepted", "bucket": bucket, "key": key}

        logger.info("Unknown SNS message type received")
        return {"status": "ignored"}
    except Exception as e:
        logger.error(f"Error processing notification: {str(e)}")
        return {"error": "processing failed"}

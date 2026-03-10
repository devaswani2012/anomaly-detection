import os
import json
import boto3
import urllib.request
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
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()

        summary = {
            col: {
                "count": baseline.get(col, {}).get("count"),
                "mean": baseline.get(col, {}).get("mean"),
                "std": baseline.get(col, {}).get("std"),
            }
            for col in ["temperature", "humidity", "pressure", "wind_speed"]
            if col in baseline
        }

        return summary
    except Exception as e:
        logger.error(f"Error generating anomaly summary: {str(e)}")
        return {"error": "Failed to generate summary"}


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

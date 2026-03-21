import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OPENAQ_BASE_URL = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3/measurements")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")


def fetch_openaq_records(limit: int = 100, page: int = 1) -> list[dict[str, Any]]:
    """Fetch one page of OpenAQ measurements."""
    headers = {"accept": "application/json"}
    if OPENAQ_API_KEY:
        headers["X-API-Key"] = OPENAQ_API_KEY

    params = {
        "limit": limit,
        "page": page,
    }

    response = requests.get(OPENAQ_BASE_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("results", [])


def upload_to_s3(records: list[dict[str, Any]]) -> str:
    """Upload OpenAQ payload to date-partitioned S3 path."""
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is required")

    now = datetime.now(timezone.utc)
    key = (
        f"aqi/raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"openaq_{now.strftime('%H%M%S')}.json"
    )

    payload = {
        "source": "openaq",
        "fetched_at": now.isoformat(),
        "record_count": len(records),
        "records": records,
    }

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )
    logger.info("Uploaded %s OpenAQ records to s3://%s/%s", len(records), S3_BUCKET, key)
    return key


def run_ingestion() -> str:
    """Run OpenAQ ingestion and return uploaded S3 key."""
    logging.basicConfig(level=logging.INFO)
    records = fetch_openaq_records()
    return upload_to_s3(records)


if __name__ == "__main__":
    uploaded_key = run_ingestion()
    print(f"Done: {uploaded_key}")

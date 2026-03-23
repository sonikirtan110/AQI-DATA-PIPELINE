import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, SSLError, Timeout

load_dotenv(override=True)

logger = logging.getLogger(__name__)

API_KEY = os.getenv("OGD_API_KEY")
RESOURCE_ID = os.getenv("OGD_RESOURCE_ID", "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69")
S3_BUCKET = os.getenv("S3_BUCKET")
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
MAX_HTTP_ATTEMPTS = int(os.getenv("OGD_HTTP_ATTEMPTS", "4"))
RETRY_BACKOFF_SECONDS = float(os.getenv("OGD_RETRY_BACKOFF_SECONDS", "2"))


def _fetch_batch(params: dict[str, Any]) -> dict[str, Any]:
    """Fetch one OGD batch with retry/backoff for transient network issues."""
    last_error: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except (SSLError, Timeout, ConnectionError) as exc:
            last_error = exc
            if attempt == MAX_HTTP_ATTEMPTS:
                break
            sleep_s = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "OGD fetch transient error on attempt %s/%s: %s. Retrying in %.1fs",
                attempt,
                MAX_HTTP_ATTEMPTS,
                exc,
                sleep_s,
            )
            time.sleep(sleep_s)

    raise RuntimeError(
        "OGD API request failed after retries. Check outbound HTTPS from the Airflow container to api.data.gov.in."
    ) from last_error


def fetch_all_records(
    city: str | None = None,
    limit: int = 100,
    offset: int = 0,
    paginate: bool = True,
) -> list[dict[str, Any]]:
    """Fetch records from the OGD API.

    Set paginate=False to fetch only a single batch (limit/offset), which is
    useful for faster daily or test runs.
    """
    if not API_KEY:
        raise ValueError("OGD_API_KEY is required")

    records: list[dict[str, Any]] = []
    current_offset = offset

    while True:
        params = {
            "api-key": API_KEY,
            "format": "json",
            "limit": limit,
            "offset": current_offset,
        }
        if city:
            params["filters[city]"] = city

        data = _fetch_batch(params)
        batch = data.get("records", [])
        if not batch:
            break

        records.extend(batch)
        total = int(data.get("total", 0))
        logger.info("Fetched %s/%s OGD records", len(records), total)

        if not paginate:
            break

        current_offset += limit
        if current_offset >= total:
            break

    return records


def upload_to_s3(records: list[dict[str, Any]]) -> str:
    """Upload records as a payload JSON document to S3 date partitions."""
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is required")
    if "yourname" in S3_BUCKET.lower():
        raise ValueError(
            "S3_BUCKET looks like a placeholder ('aqi-pipeline-yourname'). Set it to a real existing bucket."
        )

    now = datetime.now(timezone.utc)
    key = (
        f"raw/ogd/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"ogd_{now.strftime('%H%M%S')}.json"
    )

    payload = {
        "source": "ogd_india",
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

    logger.info("Uploaded %s records to s3://%s/%s", len(records), S3_BUCKET, key)
    return key


def run_ingestion(
    *,
    limit: int = 100,
    offset: int = 0,
    paginate: bool = True,
) -> tuple[str, int]:
    """Run end-to-end OGD ingestion and return uploaded S3 key and record count."""
    logging.basicConfig(level=logging.INFO)
    records = fetch_all_records(limit=limit, offset=offset, paginate=paginate)
    s3_key = upload_to_s3(records)
    return s3_key, len(records)


def fetch_and_upload_ogd(
    limit: int | None = None,
    offset: int | None = None,
    paginate: bool = False,
) -> dict[str, Any]:
    """Compatibility wrapper for DAG/tests expecting a dict payload."""
    effective_limit = limit if limit is not None else 100
    effective_offset = offset if offset is not None else 0
    # DAG/task calls should be bounded and quick by default.
    s3_key, record_count = run_ingestion(
        limit=effective_limit,
        offset=effective_offset,
        paginate=paginate,
    )
    return {
        "status": "success",
        "s3_key": s3_key,
        "records_uploaded": record_count,
    }


if __name__ == "__main__":
    uploaded_key, _ = run_ingestion()
    print(f"Done: {uploaded_key}")
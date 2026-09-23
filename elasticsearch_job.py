#!/usr/bin/env python3
"""
Parquet -> Elasticsearch ingestion job.

Walks the central station's Parquet archive (partitioned as
station_id=<id>/date=<yyyy-mm-dd>/part-<ts>-<uuid>.parquet),

"""

import argparse
import logging
import re
import signal
import sys
import time
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

# --------------------------------------------------------------------------
# Configuration (env vars, with sane local defaults)
# --------------------------------------------------------------------------

import os

PARQUET_DIR = Path(os.environ.get("PARQUET_OUTPUT_DIR", "./data/parquet"))
STATE_FILE = Path(os.environ.get("PARQUET_INGEST_STATE_FILE", "./data/parquet-ingest-state.txt"))
ES_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
ES_INDEX = os.environ.get("ELASTICSEARCH_INDEX", "weather-status")

FILENAME_RE = re.compile(r"^part-(\d+)-.*\.parquet$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("parquet-to-elastic")

# --------------------------------------------------------------------------
# Index setup
# --------------------------------------------------------------------------

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "station_id": {"type": "long"},
            "s_no": {"type": "long"},
            "battery_status": {"type": "keyword"},
            "status_timestamp": {"type": "date", "format": "epoch_second||epoch_millis"},
            "weather": {
                "properties": {
                    "humidity": {"type": "integer"},
                    "temperature": {"type": "integer"},
                    "wind_speed": {"type": "integer"},
                }
            },
        }
    }
}


def ensure_index(es: Elasticsearch) -> None:


    if es.indices.exists(index=ES_INDEX):
        return
    log.info("Index '%s' does not exist, creating it", ES_INDEX)
    es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)

def read_last_seen() -> int:
    if STATE_FILE.exists():
        try:
            return int(STATE_FILE.read_text().strip())
        except ValueError:
            log.warning("State file %s is corrupt, treating as 0", STATE_FILE)
    return 0


def write_last_seen(ts: int) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(str(ts))
    tmp.replace(STATE_FILE)  # atomic rename, same reasoning as the Parquet writer


# --------------------------------------------------------------------------
# File discovery
# --------------------------------------------------------------------------


def extract_timestamp(path: Path) -> int:
    match = FILENAME_RE.match(path.name)
    if not match:
        raise ValueError(f"filename does not match expected pattern: {path.name}")
    return int(match.group(1))


def find_new_files(last_seen: int) -> list[Path]:

    if not PARQUET_DIR.exists():
        log.warning("Parquet directory %s does not exist yet", PARQUET_DIR)
        return []

    candidates = []
    for path in PARQUET_DIR.rglob("*.parquet"):
        try:
            ts = extract_timestamp(path)
        except ValueError:
            log.warning("Skipping unrecognized file: %s", path)
            continue
        if ts > last_seen:
            candidates.append((ts, path))

    candidates.sort(key=lambda pair: pair[0])
    return [path for _, path in candidates]


# --------------------------------------------------------------------------
# Reading + indexing
# --------------------------------------------------------------------------


def rows_from_file(path: Path) -> Iterator[dict]:
    """Yield bulk-API action dicts for every row in one Parquet file, mapping fields to snake_case."""
    table = pq.read_table(path)
    for raw_row in table.to_pylist():
        # Fallback field extraction supporting both camelCase and snake_case
        station_id = raw_row.get("station_id") if raw_row.get("station_id") is not None else raw_row.get("stationId")
        s_no = raw_row.get("s_no") if raw_row.get("s_no") is not None else raw_row.get("sequenceNumber")

        if station_id is None or s_no is None:
            log.warning("Row in %s missing station_id/s_no, skipping: %r", path, raw_row)
            continue

        # Extract and handle bytes decoding for battery_status
        raw_battery = raw_row.get("battery_status", raw_row.get("batteryStatus"))
        if isinstance(raw_battery, bytes):
            battery_status = raw_battery.decode("utf-8")
        else:
            battery_status = raw_battery

        # Extract timestamp (convert ms to sec if needed)
        raw_ts = raw_row.get("status_timestamp", raw_row.get("statusTimeStamp"))
        if raw_ts is not None and raw_ts > 1e11:
            status_timestamp = int(raw_ts / 1000)
        else:
            status_timestamp = raw_ts

        # Extract nested weather struct
        raw_weather = raw_row.get("weather", raw_row.get("weatherStatus", {})) or {}
        weather = {
            "humidity": raw_weather.get("humidity"),
            "temperature": raw_weather.get("temperature"),
            "wind_speed": raw_weather.get("wind_speed", raw_weather.get("windSpeed")),
        }

        # Build normalized Elasticsearch source document
        doc = {
            "station_id": station_id,
            "s_no": s_no,
            "battery_status": battery_status,
            "status_timestamp": status_timestamp,
            "weather": weather,
        }

        yield {
            "_index": ES_INDEX,
            "_id": f"{station_id}-{s_no}",
            "_source": doc,
        }


def index_file(es: Elasticsearch, path: Path) -> int:
    actions = list(rows_from_file(path))
    if not actions:
        return 0
    try:
        success_count, errors = bulk(es, actions, raise_on_error=False, stats_only=False)
    except BulkIndexError as e:
        # some documents failed; log and continue rather than aborting the whole run
        log.error("Bulk indexing had %d failing documents in %s", len(e.errors), path)
        for err in e.errors[:5]:
            log.error("  %s", err)
        return success_count if isinstance(success_count, int) else 0

    if errors:
        log.error("%d documents failed to index from %s", len(errors), path)
        for err in errors[:5]:
            log.error("  %s", err)

    return success_count


# --------------------------------------------------------------------------
# Main run
# --------------------------------------------------------------------------


def run_once() -> None:
    es = Elasticsearch(ES_URL)
    if not es.ping():
        log.error("Could not reach Elasticsearch at %s", ES_URL)
        return

    ensure_index(es)

    last_seen = read_last_seen()
    new_files = find_new_files(last_seen)

    if not new_files:
        log.info("No new files since last_seen=%d", last_seen)
        return

    log.info("Found %d new file(s) to index", len(new_files))

    max_seen = last_seen
    total_docs = 0

    for path in new_files:
        ts = extract_timestamp(path)
        try:
            indexed = index_file(es, path)
            total_docs += indexed
            log.info("Indexed %d docs from %s", indexed, path)

            max_seen = ts
        except Exception:
            log.exception("Failed to process %s, stopping this run", path)
            break

    if max_seen > last_seen:
        write_last_seen(max_seen)
        log.info(
            "Run complete: %d docs indexed, watermark advanced %d -> %d",
            total_docs, last_seen, max_seen,
        )
    else:
        log.warning("Run complete but watermark did not advance")


def run_watch(interval_seconds: int) -> None:
    stop = {"flag": False}

    def handle_signal(signum, _frame):
        log.info("Received signal %d, will stop after current cycle", signum)
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info("Starting watch loop, interval=%ds", interval_seconds)
    while not stop["flag"]:
        try:
            run_once()
        except Exception:
            log.exception("Unhandled error during run_once(), will retry next cycle")

        for _ in range(interval_seconds):
            if stop["flag"]:
                break
            time.sleep(1)

    log.info("Watch loop stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Run a single ingestion pass and exit")
    mode.add_argument("--watch", action="store_true", help="Run continuously on a fixed interval")
    parser.add_argument(
        "--interval", type=int, default=30,
        help="Seconds between passes in --watch mode (default: 30)",
    )
    args = parser.parse_args()

    log.info(
        "Config: PARQUET_DIR=%s STATE_FILE=%s ES_URL=%s ES_INDEX=%s",
        PARQUET_DIR, STATE_FILE, ES_URL, ES_INDEX,
    )

    if args.once:
        run_once()
    else:
        run_watch(args.interval)


if __name__ == "__main__":
    sys.exit(main() or 0)
"""
Optimized USDA FDC -> DynamoDB pipeline.

Key optimizations over final2_ddb.py:
  - Batch API: POST /v1/foods (200 IDs per request) instead of 450K individual GETs
  - Concurrency: 2 API fetch threads + 1 DynamoDB writer thread
  - Batch DynamoDB writes: table.batch_writer() (25 items per batch, auto-retry)
  - Connection pooling: requests.Session with HTTPAdapter
  - Checkpoint/resume: completed_ids.txt for restartability
"""

import os
import csv
import json
import time
import queue
import logging
import decimal
import argparse
import threading
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

from dynamo_db_script import HIERARCHY, PARENT_LOOKUP, build_nutrient_lookup

# ── Configuration ────────────────────────────────────────────────────────────
API_BATCH_SIZE = 50         # USDA API returns max 50 items per POST /v1/foods
API_CONCURRENCY = 2         # Number of parallel API fetch threads
RATE_LIMIT_PER_SEC = 0.95   # Requests per second (stays under 3,600/hr)
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 2     # seconds
CHECKPOINT_FILE = "completed_ids.txt"

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ── Reused from final2_ddb.py ────────────────────────────────────────────────

_NBR_TO_NID = {nbr: nid for nid, name, unit, nbr, parent_id, level, group in HIERARCHY if nbr}

# Pre-compute children map for O(n) hierarchy traversal instead of O(n^2)
_CHILDREN_MAP = {}
for _nid, _name, _unit, _nbr, _parent_id, _level, _group in HIERARCHY:
    _CHILDREN_MAP.setdefault(_parent_id, []).append(_nid)


def build_nutrient_lookup_abridged(raw_food_nutrients: list) -> dict:
    lookup = {}
    for fn in raw_food_nutrients:
        nbr = fn.get("number")
        if nbr and nbr in _NBR_TO_NID:
            nid = _NBR_TO_NID[nbr]
            if nid not in lookup:
                lookup[nid] = fn
    return lookup


def _has_data_or_children(nid, nutrient_lookup):
    """Check if nutrient or any descendant has data. Uses pre-computed _CHILDREN_MAP."""
    if nid in nutrient_lookup:
        return True
    for child_nid in _CHILDREN_MAP.get(nid, []):
        if _has_data_or_children(child_nid, nutrient_lookup):
            return True
    return False


def process_api_response(data):
    """
    Process parsed JSON from the USDA API response.
    Identical logic to final2_ddb.py but uses optimized hierarchy traversal.
    """
    fdc_id = data.get("fdcId")
    description = data.get("description") or "No description"

    out_data = {"fdcId": fdc_id, "description": description}

    raw_nutrients = data.get("foodNutrients", [])
    is_abridged = bool(raw_nutrients and "number" in raw_nutrients[0] and "nutrient" not in raw_nutrients[0])

    if is_abridged:
        nutrient_lookup = build_nutrient_lookup_abridged(raw_nutrients)
    else:
        nutrient_lookup = build_nutrient_lookup(raw_nutrients)

    for entry in HIERARCHY:
        nid, name, unit, nbr, parent_id, level, group = entry

        if not _has_data_or_children(nid, nutrient_lookup):
            continue

        fn_data = nutrient_lookup.get(nid)
        amount = fn_data.get("amount") if fn_data is not None else None

        parent_info = PARENT_LOOKUP.get(nid, {})

        entry_dict = {"nutrient_id": nid, "name": name, "unit": unit}
        if amount is not None:
            entry_dict["value"] = amount

        pid = parent_info.get("parent_id")
        if pid is not None:
            entry_dict["parent_nutrient_id"] = pid
            entry_dict["parent_nutrient_name"] = parent_info.get("parent_name")

        out_data[name.lower()] = entry_dict

    return out_data


def get_dynamodb_table():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, '.env'))

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region_name = os.getenv('AWS_REGION', 'us-east-1')

    if not aws_access_key_id or not aws_secret_access_key:
        logging.warning("AWS credentials not found. Assuming default credentials or IAM role.")

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    return dynamodb.Table('food-nutrients')


def read_fdc_ids_from_csv(csv_file_path):
    fdc_ids = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)

            col_idx = 0
            if header:
                for idx, col in enumerate(header):
                    if 'fdcid' in col.strip().lower():
                        col_idx = idx
                        break
                if col_idx == 0 and header[0].strip().isdigit():
                    fdc_ids.append(header[0].strip())

            for row in reader:
                if row and len(row) > col_idx:
                    val = row[col_idx].strip()
                    if val:
                        fdc_ids.append(val)
    except Exception as e:
        logging.error(f"Failed to read CSV file '{csv_file_path}': {e}")
    return fdc_ids


# ── Rate Limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Thread-safe rate limiter using a simple token-bucket approach."""

    def __init__(self, max_per_second: float):
        self.min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


# ── Checkpoint ───────────────────────────────────────────────────────────────

class Checkpoint:
    """Thread-safe checkpoint tracker for resume capability."""

    def __init__(self, filepath):
        self.filepath = filepath
        self._lock = threading.Lock()
        self._completed = set()
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, 'r') as f:
                self._completed = {line.strip() for line in f if line.strip()}
            logging.info(f"Checkpoint loaded: {len(self._completed)} IDs already completed.")

    def is_done(self, fdc_id):
        return str(fdc_id) in self._completed

    def mark_done(self, fdc_ids):
        with self._lock:
            with open(self.filepath, 'a') as f:
                for fid in fdc_ids:
                    f.write(f"{fid}\n")
            self._completed.update(str(fid) for fid in fdc_ids)

    @property
    def count(self):
        return len(self._completed)


# ── Batch API Fetcher ────────────────────────────────────────────────────────

def create_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
    session.mount("https://", adapter)
    return session


def fetch_batch(session, fdc_ids_chunk, api_key, rate_limiter):
    """
    Fetch up to 200 foods via POST /v1/foods.
    Returns (list_of_food_dicts, list_of_missing_ids, error_or_None).
    """
    rate_limiter.acquire()

    url = f"https://api.nal.usda.gov/fdc/v1/foods?api_key={api_key}"
    # The API expects integer fdcIds
    payload = {
        "fdcIds": [int(fid) for fid in fdc_ids_chunk],
        "format": "abridged",
    }

    retry_delay = INITIAL_RETRY_DELAY
    requested_ids = set(str(fid) for fid in fdc_ids_chunk)

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(url, json=payload, timeout=60)

            if response.status_code == 200:
                foods = json.loads(response.text, parse_float=decimal.Decimal)
                returned_ids = {str(f.get("fdcId")) for f in foods}
                missing_ids = requested_ids - returned_ids
                return foods, list(missing_ids), None

            elif response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Rate limited (429). Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    rate_limiter.acquire()  # Re-acquire after backoff
                    continue
                else:
                    return [], [], f"Rate limited after {MAX_RETRIES} retries"

            elif response.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Server error ({response.status_code}). Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                else:
                    return [], [], f"Server error {response.status_code} after {MAX_RETRIES} retries"

            else:
                return [], [], f"HTTP {response.status_code}"

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"Network error: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                return [], [], f"Network error after {MAX_RETRIES} retries: {e}"

    return [], [], "Exhausted retries"


# ── DynamoDB Writer Thread ───────────────────────────────────────────────────

def dynamo_writer(write_queue, table, checkpoint, failed_ids, failed_lock, counters, counters_lock, stop_event):
    """
    Consumer thread: pulls transformed items from the queue and batch-writes to DynamoDB.
    """
    buffer = []
    FLUSH_SIZE = 25

    def flush(items):
        if not items:
            return
        try:
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            written_ids = [str(item["fdcId"]) for item in items]
            checkpoint.mark_done(written_ids)
            with counters_lock:
                counters["success"] += len(items)
        except ClientError as e:
            logging.error(f"DynamoDB batch write error: {e.response['Error']['Message']}")
            with failed_lock:
                for item in items:
                    failed_ids.append((item["fdcId"], "DynamoDB error"))

    while not stop_event.is_set() or not write_queue.empty():
        try:
            item = write_queue.get(timeout=1)
            buffer.append(item)
            if len(buffer) >= FLUSH_SIZE:
                flush(buffer)
                buffer = []
        except queue.Empty:
            continue

    # Flush remaining items
    flush(buffer)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Optimized USDA FDC -> DynamoDB pipeline")
    parser.add_argument("csv_file", help="Path to CSV file (or filename inside fdcIds/ folder)")
    args = parser.parse_args()

    # Resolve CSV path
    csv_file_path = args.csv_file
    if not os.path.exists(csv_file_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file_path = os.path.join(script_dir, "fdcIds", args.csv_file)
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file '{args.csv_file}' not found.")
        return

    # Load IDs
    all_ids = read_fdc_ids_from_csv(csv_file_path)
    if not all_ids:
        logging.warning("No fdcId values found. Exiting.")
        return
    logging.info(f"Total IDs in CSV: {len(all_ids)}")

    # Checkpoint: filter out already-completed IDs
    script_dir = os.path.dirname(os.path.abspath(__file__))
    checkpoint = Checkpoint(os.path.join(script_dir, CHECKPOINT_FILE))
    remaining_ids = [fid for fid in all_ids if not checkpoint.is_done(fid)]
    logging.info(f"Already completed: {checkpoint.count}. Remaining: {len(remaining_ids)}")

    if not remaining_ids:
        logging.info("All IDs already processed. Nothing to do.")
        return

    # Setup
    table = get_dynamodb_table()
    api_key = "f8r37KRDp65vbKLqG0bN710NdhCnWc2aIG51jTVf"
    rate_limiter = RateLimiter(RATE_LIMIT_PER_SEC)
    session = create_session()

    # Shared state
    write_queue = queue.Queue(maxsize=500)
    failed_ids = []
    failed_lock = threading.Lock()
    counters = {"success": 0, "api_fetched": 0}
    counters_lock = threading.Lock()
    stop_event = threading.Event()

    # Start DynamoDB writer thread
    writer_thread = threading.Thread(
        target=dynamo_writer,
        args=(write_queue, table, checkpoint, failed_ids, failed_lock, counters, counters_lock, stop_event),
        daemon=True,
    )
    writer_thread.start()

    # Chunk IDs into batches of 200
    chunks = [remaining_ids[i:i + API_BATCH_SIZE] for i in range(0, len(remaining_ids), API_BATCH_SIZE)]
    total_items = len(remaining_ids)
    start_time = time.time()

    logging.info(f"Starting: {len(chunks)} API batches, {API_CONCURRENCY} threads, rate limit {RATE_LIMIT_PER_SEC}/sec")

    # Process batches with thread pool
    with ThreadPoolExecutor(max_workers=API_CONCURRENCY) as executor:
        future_to_chunk = {
            executor.submit(fetch_batch, session, chunk, api_key, rate_limiter): chunk
            for chunk in chunks
        }

        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                foods, missing_ids, error = future.result()

                # Log missing IDs — do NOT mark as done; they need to be retried.
                # The USDA API silently caps batch responses at 50 items,
                # so "missing" usually means the batch was too large, not a true 404.
                if missing_ids:
                    logging.warning(f"{len(missing_ids)} IDs missing from batch response (will retry on next run)")
                    with failed_lock:
                        for mid in missing_ids:
                            failed_ids.append((mid, "not found in batch response"))

                # Log batch-level errors
                if error:
                    logging.error(f"Batch error ({len(chunk)} IDs): {error}")
                    with failed_lock:
                        for fid in chunk:
                            failed_ids.append((fid, error))
                    continue

                # Transform and enqueue each food for DynamoDB writing
                for food_data in foods:
                    fdc_id = food_data.get("fdcId")
                    try:
                        out_data = process_api_response(food_data)
                        write_queue.put(out_data)
                    except Exception as e:
                        logging.error(f"Transform error for fdcId {fdc_id}: {e}")
                        with failed_lock:
                            failed_ids.append((fdc_id, f"transform error: {e}"))

                with counters_lock:
                    counters["api_fetched"] += len(foods)

                # Progress logging
                with counters_lock:
                    fetched = counters["api_fetched"]
                    written = counters["success"]
                if fetched % 1000 < API_BATCH_SIZE:
                    elapsed = time.time() - start_time
                    rate = fetched / elapsed if elapsed > 0 else 0
                    eta = (total_items - fetched) / rate / 60 if rate > 0 else 0
                    logging.info(f"Progress: {fetched}/{total_items} fetched, {written} written ({rate:.1f}/sec, ETA: {eta:.0f}min)")

            except Exception as e:
                logging.exception(f"Unexpected error processing batch: {e}")
                with failed_lock:
                    for fid in chunk:
                        failed_ids.append((fid, f"exception: {e}"))

    # Signal writer thread to finish and wait
    stop_event.set()
    writer_thread.join(timeout=120)

    # Final summary
    elapsed = time.time() - start_time
    logging.info(f"\n=== RUN SUMMARY ==================================")
    logging.info(f"  Total IDs in CSV    : {len(all_ids)}")
    logging.info(f"  Previously done     : {len(all_ids) - len(remaining_ids)}")
    logging.info(f"  Processed this run  : {len(remaining_ids)}")
    logging.info(f"  Succeeded           : {counters['success']}")
    logging.info(f"  Failed              : {len(failed_ids)}")
    logging.info(f"  Elapsed             : {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logging.info(f"==================================================")

    if failed_ids:
        log_path = os.path.join(script_dir, f"failed_fdc_ids_{date.today()}.log")
        with open(log_path, 'w') as lf:
            lf.write(f"# Failed fdcIds — {date.today()}\n")
            lf.write(f"# These IDs could not be fetched or written to DynamoDB.\n\n")
            for fid, reason in failed_ids:
                lf.write(f"{fid}\t{reason}\n")
        logging.info(f"  Failed ID list written to: {log_path}")


if __name__ == "__main__":
    main()

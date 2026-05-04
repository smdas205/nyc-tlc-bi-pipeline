import logging
import os
import re
import sys
from pathlib import Path

import pyarrow.parquet as pq
import redis
import requests
from tenacity import retry, stop_after_attempt, wait_exponential


# Redis Ping Function
def check_redis(r: redis.Redis) -> bool:
    return r.ping()


# Other Initializations
DOWNLOAD_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
DOWNLOAD_FOLDER = "/opt/airflow/data/raw/"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
logger = logging.getLogger(__name__)


# Redis Sets
MISSING_SET_KEY = "tlc:missing"
DOWNLOADED_SET_KEY = "tlc:downloaded"


# Function to Set Filename Redis HSET
def redis_file_hset(filename: str) -> str:
    pattern = r"([^_]+)_.*_(\d{4})-(\d{2})\.parquet"
    match = re.search(pattern, filename)
    file_type = ""
    yyyy = 0
    mm = 0
    if match:
        file_type = match.group(1)
        yyyy = match.group(2)
        mm = match.group(3)
    return f"tlc:file:{file_type}:{yyyy}:{mm}"


# Function to check URL status
@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def check(url: str) -> bool:
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if response.status_code == 200:
            logger.info(f"File Exists at {url}")
        return response.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Network Error For {url}: {e} ")
        return False


# Function to Download Files from URL in chunks
def download_files(url: str, dest_path: str) -> bool:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        with requests.get(url, stream=True, timeout=60, headers=headers) as url_resp:
            url_resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in url_resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        logger.info(f"\nFile Downloaded from {url}.")
        return True
    except requests.RequestException as e:
        logger.error(f"Download Failed: {e}.")
        return False


# Function to Check if PARQUET files are valid
def parquet_check(full_file_path: str) -> bool:
    try:
        pq.read_metadata(full_file_path)
        return True
    except Exception as e:
        logger.error(f"File {full_file_path} is corrupted or invalid : {e}")
        return False


# Function to Parse Downloaded PARQUET Files
def parse_files(file_path: str) -> dict[str, str]:
    file_size = os.path.getsize(file_path)  # Get File Size
    row_count = pq.ParquetFile(file_path).metadata.num_rows  # Get File Row Count
    return {"file_size": str(file_size), "row_count": str(row_count)}


# Function to Mark Missing Files in URLs
def mark_missing(r: redis.Redis, file_type: str, year: int, month: str) -> None:
    result = r.sadd(MISSING_SET_KEY, f"{file_type}:{year}:{month}")
    if result == 1:
        logger.info(f"Added to {MISSING_SET_KEY} : {file_type} : {year} : {month}")
    elif result == 0:
        logger.info(f"Key already added to {MISSING_SET_KEY}.")
    else:
        logger.error(f"Failed to add to {MISSING_SET_KEY}.")


# Function to Mark Downloaded Files in URLs
def mark_download(r: redis.Redis, file_type: str, year: int, month: str) -> None:
    result = r.sadd(DOWNLOADED_SET_KEY, f"{file_type}:{year}:{month}")
    if result == 1:
        logger.info(f"Added to {DOWNLOADED_SET_KEY} : {file_type} : {year} : {month}")
    elif result == 0:
        logger.info(f"Key already added to {DOWNLOADED_SET_KEY}.")
    else:
        logger.error(f"Failed to add to {DOWNLOADED_SET_KEY}.")


# Function to Add File Metadata to Redis Hash
def add_file_data(r: redis.Redis, filename: str, metadata: dict) -> None:
    try:
        file_hash = redis_file_hset(filename)
        result = r.hset(file_hash, mapping=metadata)
        if result > 0:
            logger.info(f"Added {result} fields to {file_hash}.")
        else:
            logger.info(f"No new fields added to {file_hash}.")
    except redis.RedisError as e:
        logger.error(f"Redis Error while adding file data: {e}")


# Main Function
def download(
    r: redis.Redis, file_type: str, year: int, month: str, label: str, **kwargs
) -> None:

    logger.info(f"Starting execution for trip file_type: {file_type} for {label}:")
    try:
        url = f"{DOWNLOAD_URL}{file_type}_tripdata_{label}.parquet"
        file_name = f"{file_type}_tripdata_{label}.parquet"
        full_file_path = f"{DOWNLOAD_FOLDER}{file_type}_tripdata_{label}.parquet"
        if check_redis(r):
            print("Redis Connected.")
        else:
            sys.exit("Error: Redis Not Connected.")
        if not check(url):
            mark_missing(r, file_type, year, month)
        elif check(url):
            if download_files(url, full_file_path) and parquet_check(full_file_path):
                mark_download(r, file_type, year, month)
                add_file_data(r, file_name, parse_files(full_file_path))
            else:
                Path(full_file_path).unlink(missing_ok=True)
                logger.error(f"File not saved : {file_type},{year},{month}")
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise

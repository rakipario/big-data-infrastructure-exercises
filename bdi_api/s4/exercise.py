import concurrent.futures
import gzip
import io
import json
import os
from typing import Annotated, List, Dict

import boto3
import duckdb as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.config import Config
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, status, Query
from tqdm import tqdm

from bdi_api.settings import Settings

settings = Settings()

s3_client = boto3.client(
    "s3",
    config=Config(
        retries={"max_attempts": 3},
        connect_timeout=10,
        read_timeout=30
    )
)

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

def clean_directory(directory: str) -> None:
    """Cleans the given directory by removing its contents."""
    if os.path.exists(directory):
        for file in os.listdir(directory):
            os.remove(os.path.join(directory, file))
    os.makedirs(directory, exist_ok=True)

def process_aircraft_data(data: dict) -> List[Dict]:
    """Process raw aircraft data into structured format (same as S1)."""
    return [{
        "icao": ac.get("hex"),
        "registration": ac.get("r"),
        "type": ac.get("t"),
        "lat": ac.get("lat"),
        "lon": ac.get("lon"),
        "altitude_baro": ac.get("alt_baro", 0) if ac.get("alt_baro") != "ground" else 0,
        "ground_speed": ac.get("gs", 0),
        "had_emergency": ac.get("emergency", "none") not in ["none", None],
        "timestamp": data.get("now", 0)
    } for ac in data.get("aircraft", [])]

@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int, 
        Query(..., description="Number of files to download from source")
    ] = 1000
) -> str:
    """Download files directly to S3 without local storage."""
    base_url = f"{settings.source_url}/2023/11/01/"
    s3_prefix = "raw/day=20231101/"

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a") 
            if a["href"].endswith(".json.gz")
        ][:file_limit]
    except requests.RequestException as e:
        raise HTTPException(500, f"Source access failed: {str(e)}")

    def upload_to_s3(file_name: str) -> bool:
        try:
            file_url = f"{base_url}{file_name}"
            with requests.get(file_url, stream=True, timeout=15) as response:
                response.raise_for_status()
                s3_client.put_object(
                    Bucket=settings.s3_bucket,
                    Key=f"{s3_prefix}{file_name}",
                    Body=response.content
                )
            return True
        except Exception as e:
            print(f"Failed {file_name}: {str(e)}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(
            executor.map(upload_to_s3, files),
            total=len(files),
            desc="Uploading to S3"
        ))

    success_count = sum(results)
    return f"Successfully stored {success_count}/{len(files)} files in {settings.s3_bucket}"

@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Process S3 data into Parquet format and save locally and to S3."""
    s3_prefix = "raw/day=20231101/"
    local_dir = os.path.join(settings.prepared_dir, "day=20231101")
    local_path = os.path.join(local_dir, 'aircraft_data.parquet')
    s3_output_key = "prepared/day=20231101/aircraft_data.parquet"

    clean_directory(local_dir)

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        all_data = []

        def process_s3_file(s3_key: str) -> List[Dict]:
            try:
                response = s3_client.get_object(Bucket=settings.s3_bucket, Key=s3_key)
                content = response["Body"].read()
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz_file:
                    data = json.load(gz_file)
                return process_aircraft_data(data)
            except Exception as e:
                print(f"Failed {s3_key}: {str(e)}")
                return []

        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=s3_prefix):
            s3_keys = [obj['Key'] for obj in page.get("Contents", [])]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                page_data = list(tqdm(
                    executor.map(process_s3_file, s3_keys),
                    total=len(s3_keys),
                    desc="Processing S3 Files"
                ))
                all_data.extend([item for sublist in page_data if sublist for item in sublist])

        if not all_data:
            raise Exception("No valid data processed")

        df = pd.DataFrame(all_data)
        table = pa.Table.from_pandas(df)

        # Save locally
        pq.write_table(table, local_path)

        # Save to S3
        with io.BytesIO() as buffer:
            pq.write_table(table, buffer)
            s3_client.put_object(
                Bucket=settings.s3_bucket,
                Key=s3_output_key,
                Body=buffer.getvalue()
            )

        return f"Processed {len(all_data)} records to {local_path} and s3://{settings.s3_bucket}/{s3_output_key}"

    except Exception as e:
        raise HTTPException(500, f"Preparation failed: {str(e)}")

@s4.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> List[Dict]:
    """List aircraft from local Parquet file."""
    local_path = os.path.join(settings.prepared_dir, "day=20231101", 'aircraft_data.parquet')
    query = f"""
        SELECT DISTINCT icao, registration, type 
        FROM '{local_path}'
        ORDER BY icao
        LIMIT {num_results}
        OFFSET {page * num_results}
    """
    result = db.query(query).df()
    return result.to_dict(orient='records')

@s4.get("/aircraft/{icao}/positions")
def get_aircraft_positions(icao: str, num_results: int = 1000, page: int = 0) -> List[Dict]:
    """Get positions from local Parquet file."""
    local_path = os.path.join(settings.prepared_dir, "day=20231101", 'aircraft_data.parquet')
    query = f"""
        SELECT timestamp, lat, lon 
        FROM '{local_path}'
        WHERE icao = '{icao}'
        ORDER BY timestamp
        LIMIT {num_results}
        OFFSET {page * num_results}
    """
    result = db.query(query).df()
    return result.to_dict(orient='records')

@s4.get("/aircraft/{icao}/stats")
def get_aircraft_stats(icao: str) -> Dict:
    """Get stats from local Parquet file."""
    local_path = os.path.join(settings.prepared_dir, "day=20231101", 'aircraft_data.parquet')
    query = f"""
        SELECT 
            COALESCE(MAX(altitude_baro), 0) AS max_altitude_baro,
            COALESCE(MAX(ground_speed), 0) AS max_ground_speed,
            COALESCE(CAST(MAX(had_emergency) AS BOOLEAN), FALSE) AS had_emergency
        FROM '{local_path}'
        WHERE icao = '{icao}'
    """
    result = db.query(query).df()
    return result.iloc[0].to_dict() if not result.empty else {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

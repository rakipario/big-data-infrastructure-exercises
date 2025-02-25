import os
import gzip
import concurrent.futures
from typing import Annotated
import boto3
import requests
import ujson
from bs4 import BeautifulSoup
from fastapi import APIRouter, status, Query, HTTPException
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
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

def process_json_data(data):
    """Processes JSON data into a DataFrame."""
    if 'aircraft' not in data:
        return pd.DataFrame()

    timestamp = data['now']
    df = pd.DataFrame(data['aircraft'])
    df_new = pd.DataFrame()

    df_new['altitude_baro'] = df['alt_baro'].replace({'ground': 0})
    if 'emergency' in df.columns:
        df_new['had_emergency'] = df['emergency'].apply(lambda x: False if x in ["none", None] else True)
    else:
        df_new['had_emergency'] = False
    df_new['icao'] = df.get('hex', None)
    df_new['registration'] = df.get('r', None)
    df_new['type'] = df.get('t', None)
    df_new['lat'] = df.get('lat', None)
    df_new['lon'] = df.get('lon', None)
    df_new['ground_speed'] = df.get('gs', None)
    df_new['timestamp'] = timestamp

    return df_new

@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads files from the source and uploads them to an AWS S3 bucket."""
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    # Fetch the list of files to download
    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch file list: {e}")

    s3 = boto3.client('s3')

    def upload_to_s3(file):
        try:
            response = requests.get(base_url + file, timeout=10)
            response.raise_for_status()
            s3_key = s3_prefix_path + file
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=response.content)
        except Exception as e:
            print(f"Failed to upload {file}: {e}")

    # Upload files to S3 in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(upload_to_s3, file_links)

    return f"Uploaded {len(file_links)} files to s3://{s3_bucket}/{s3_prefix_path}"

@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Fetches data from AWS S3, processes it, and saves it locally."""
    s3 = boto3.client('s3')
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    # Clean and prepare the local directory
    clean_directory(prepared_dir)

    # List files in S3
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        if 'Contents' not in response:
            raise Exception("No files found in S3")
        s3_keys = [obj['Key'] for obj in response['Contents']]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list S3 objects: {e}")

    def process_s3_file(s3_key):
        try:
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            compressed_data = obj['Body'].read()
            json_data = gzip.decompress(compressed_data)
            data = ujson.loads(json_data)
            df = process_json_data(data)
            return df
        except Exception as e:
            print(f"Failed to process {s3_key}: {e}")
            return None

    # Process files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        dfs = [df for df in executor.map(process_s3_file, s3_keys) if df is not None and not df.empty]

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, prepare_file_path)
    else:
        raise HTTPException(status_code=500, detail="No valid data to process")

    return f"Data preparation complete. Saved to {prepare_file_path}"
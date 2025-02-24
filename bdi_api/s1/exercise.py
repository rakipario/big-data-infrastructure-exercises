import concurrent.futures
import os
import time
from typing import Annotated, List, Dict
from io import BytesIO

import duckdb as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import ujson
from bs4 import BeautifulSoup
from fastapi import APIRouter, status, Query, HTTPException

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

def clean_directory(directory: str) -> None:
    """Cleans the given directory by removing its contents."""
    if os.path.exists(directory):
        for file in os.listdir(directory):
            os.remove(os.path.join(directory, file))
    os.makedirs(directory, exist_ok=True)

def download_file(base_url: str, file: str, download_dir: str) -> None:
    """Downloads a single file and saves it decompressed if necessary."""
    try:
        response = requests.get(base_url + file, timeout=10)
        response.raise_for_status()
        content = response.content
        output_path = os.path.join(download_dir, file[:-3])
        if content.startswith(b'\x1f\x8b'):  # Gzip magic bytes
            import gzip
            with gzip.open(BytesIO(content), 'rb') as gz:
                with open(output_path, 'wb') as f:
                    f.write(gz.read())
        else:
            with open(output_path, 'wb') as f:
                f.write(content)
    except requests.RequestException as e:
        print(f"Failed to download {file}: {e}")

@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101."""
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    clean_directory(download_dir)

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch file list: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(download_file, base_url, file, download_dir) for file in file_links]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    return f"Downloaded {len(file_links)} files to {download_dir}"

def process_file(file_path: str) -> pd.DataFrame:
    """Processes a single file and returns a DataFrame."""
    try:
        with open(file_path) as f:
            data = ujson.load(f)
    except (ujson.JSONDecodeError, ValueError) as e:
        print(f"Skipping {file_path}: Invalid JSON ({e})")
        return pd.DataFrame()

    if 'aircraft' not in data:
        print(f"File {file_path} does not contain aircraft data")
        return pd.DataFrame()

    timestamp = data['now']
    df = pd.DataFrame(data['aircraft'])
    df_new = pd.DataFrame()

    df_new['altitude_baro'] = df['alt_baro'].replace({'ground': 0})
    df_new['had_emergency'] = df['emergency'].apply(lambda x: x not in ["none", None])
    df_new['icao'] = df.get('hex', None)
    df_new['registration'] = df.get('r', None)
    df_new['type'] = df.get('t', None)
    df_new['lat'] = df.get('lat', None)
    df_new['lon'] = df.get('lon', None)
    df_new['ground_speed'] = df.get('gs', None)
    df_new['timestamp'] = timestamp

    return df_new

@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepares the data for analysis by processing raw files and saving them in Parquet format."""
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    clean_directory(prepared_dir)

    files = [os.path.join(raw_dir, file) for file in os.listdir(raw_dir)]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(process_file, files))

    tables = [pa.Table.from_pandas(df) for df in results if not df.empty]
    if tables:
        schema = tables[0].schema
        tables = [table.cast(schema) for table in tables]
        table = pa.concat_tables(tables)
        pq.write_table(table, prepare_file_path)
    else:
        print("No valid data to process")

    return f"Data preparation complete. Saved to {prepare_file_path}"

@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> List[Dict]:
    """Lists all available aircraft, ordered by ICAO."""
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    query = f"""
        SELECT DISTINCT icao, registration, type
        FROM '{prepare_file_path}'
        ORDER BY icao ASC
        LIMIT {num_results}
        OFFSET {page * num_results}
    """
    result = db.query(query).df()
    return result.to_dict(orient='records')

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> List[Dict]:
    """Returns all known positions of an aircraft, ordered by timestamp."""
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    query = f"""
        SELECT timestamp, lat, lon
        FROM '{prepare_file_path}'
        WHERE icao = '{icao}'
        AND lat IS NOT NULL
        AND lon IS NOT NULL
        ORDER BY timestamp ASC
        LIMIT {num_results}
        OFFSET {page * num_results}
    """
    result = db.query(query).df()
    return result.to_dict(orient='records')

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> Dict:
    """Returns statistics about the aircraft."""
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    query = f"""
        SELECT
            COALESCE(MAX(altitude_baro), 0) AS max_altitude_baro,
            COALESCE(MAX(ground_speed), 0) AS max_ground_speed,
            COALESCE(CAST(MAX(had_emergency) AS BOOLEAN), FALSE) AS had_emergency
        FROM '{prepare_file_path}'
        WHERE icao = '{icao}'
    """
    result = db.query(query).df()
    return result.to_dict(orient='records')[0] if not result.empty else {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

from os.path import dirname, join
from typing import Annotated
import os
import shutil
import json
import requests
import gzip
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi import APIRouter, status, Query

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))


class DBCredentials(BaseSettings):
    """Use env variables prefixed with BDI_DB_"""

    host: str
    port: int = 5432
    username: str
    password: str
    model_config = SettingsConfigDict(env_prefix="bdi_db_")


class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default="bdi-test",
        description="Call the api like `BDI_S3_BUCKET=yourbucket poetry run uvicorn...`",
    )
    telemetry: bool = False
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    model_config = SettingsConfigDict(env_prefix="bdi_")

    @property
    def raw_dir(self) -> str:
        """Store inside all the raw JSONs."""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")


settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


# Helper Functions
def clean_directory(directory: str):
    """Cleans the given directory by removing its contents."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)


def read_json(file_path: str):
    """Reads a JSON file and returns its content."""
    with open(file_path, "r") as file:
        return json.load(file)


# Endpoints
@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[int, Query(..., description="Limits the number of files to download.")] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101."""
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Clean the directory before saving new files
    clean_directory(download_dir)

    # Print out the directory for debugging
    print(f"Saving files to: {download_dir}")
    
    # Download files
    for i in range(file_limit):
        # Assuming the files are named like 000000Z.json.gz, 000005Z.json.gz, etc.
        file_name = f"{i * 5:06d}Z.json.gz"  # Creates filenames like 000000Z.json.gz
        file_url = f"{base_url}{file_name}"
        local_file_path = os.path.join(download_dir, file_name)

        # Print out the URL and path for debugging
        print(f"Attempting to download: {file_url}")
        response = requests.get(file_url, stream=True)

        if response.status_code == 200:
            print(f"Successfully downloaded {file_url}")
            # Decompress the .gz file and save it locally
            with gzip.open(response.raw, 'rb') as f_in:
                with open(local_file_path[:-3], 'wb') as f_out:  # Remove .gz extension
                    shutil.copyfileobj(f_in, f_out)
        else:
            print(f"Failed to download {file_url} with status {response.status_code}")
            break  # Stop if there are fewer files than the limit

    return f"Downloaded {file_limit} files to {download_dir}"



@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepares the data for analysis."""
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = settings.prepared_dir

    # Clean the prepared folder
    clean_directory(prepared_dir)

    # Process raw files
    for file_name in os.listdir(raw_dir):
        raw_file_path = os.path.join(raw_dir, file_name)

        with open(raw_file_path, "r") as raw_file:
            data = json.load(raw_file)

        # Preprocess and save processed data
        # Iterate over the 'aircraft' array directly
        processed_data = [
            {
                "hex": aircraft.get("hex"),
                "type": aircraft.get("type"),
                "flight": aircraft.get("flight"),
                "registration": aircraft.get("r"),
                "icao": aircraft.get("hex"),  # Use hex as ICAO (or other identifier as needed)
                "alt_baro": aircraft.get("alt_baro"),
                "gs": aircraft.get("gs"),
                "track": aircraft.get("track"),
                "lat": aircraft.get("lat"),
                "lon": aircraft.get("lon"),
                "messages": aircraft.get("messages"),
                "seen": aircraft.get("seen"),
                "rssi": aircraft.get("rssi"),
            }
            for aircraft in data.get("aircraft", [])
        ]

        # Write the processed data to a new file
        prepared_file_path = os.path.join(prepared_dir, f"processed_{file_name}")
        with open(prepared_file_path, "w") as prepared_file:
            json.dump(processed_data, prepared_file)

    return "Data preparation complete."


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """Lists all the available aircraft."""
    prepared_dir = settings.prepared_dir
    all_aircraft = []

    # Loop through the prepared directory and load aircraft data
    for file_name in os.listdir(prepared_dir):
        prepared_file_path = os.path.join(prepared_dir, file_name)
        data = read_json(prepared_file_path)

        # Ensure the aircraft data is valid and add to the list
        for aircraft in data:
            aircraft_info = {
                "hex": aircraft.get("hex"),
                "flight": aircraft.get("flight"),
                "registration": aircraft.get("registration"),
                "type": aircraft.get("type"),
                "icao": aircraft.get("hex"),  # Using hex as ICAO identifier
                "alt_baro": aircraft.get("alt_baro"),
                "gs": aircraft.get("gs"),
                "track": aircraft.get("track"),
                "lat": aircraft.get("lat"),
                "lon": aircraft.get("lon"),
                "rssi": aircraft.get("rssi"),
            }
            all_aircraft.append(aircraft_info)

    # Sort by ICAO (hex) and paginate
    all_aircraft.sort(key=lambda x: x["icao"])
    start = page * num_results
    end = start + num_results
    return all_aircraft[start:end]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)."""
    prepared_dir = settings.prepared_dir
    all_positions = []

    for file_name in os.listdir(prepared_dir):
        prepared_file_path = os.path.join(prepared_dir, file_name)
        print(f"Reading file: {prepared_file_path}")
        
        data = read_json(prepared_file_path)
        print(f"Loaded data from {prepared_file_path}")
        
        for aircraft in data:
            # Debugging line: Check if ICAO matches
            print(f"Checking aircraft with hex {aircraft['hex']} against {icao}")

            if aircraft["icao"] == icao:
                print(f"Found matching aircraft with ICAO: {icao}")
                
                # If lat and lon are available, consider them as position
                if aircraft.get("lat") is not None and aircraft.get("lon") is not None:
                    position = {
                        "lat": aircraft["lat"],
                        "lon": aircraft["lon"],
                        "timestamp": aircraft.get("timestamp", 0),  # Add timestamp if available
                    }
                    all_positions.append(position)

    # Sort positions by timestamp if available
    all_positions.sort(key=lambda x: x["timestamp"])
    
    # Paginate results
    start = page * num_results
    end = start + num_results
    return all_positions[start:end]







@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft."""
    prepared_dir = settings.prepared_dir

    for file_name in os.listdir(prepared_dir):
        prepared_file_path = os.path.join(prepared_dir, file_name)
        data = read_json(prepared_file_path)

        for aircraft in data:
            # Use 'hex' as ICAO
            if aircraft["hex"] == icao:
                # Check if lat, lon, alt_baro, and gs exist, and calculate statistics
                max_altitude_baro = aircraft.get("alt_baro", 0)
                max_ground_speed = aircraft.get("gs", 0)
                had_emergency = aircraft.get("emergency", False)

                # For simplicity, we're using the current aircraft data to calculate stats
                return {
                    "max_altitude_baro": max_altitude_baro,
                    "max_ground_speed": max_ground_speed,
                    "had_emergency": had_emergency,
                }

    return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}



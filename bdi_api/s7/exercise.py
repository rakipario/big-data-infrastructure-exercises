import gzip
import io
import json
from typing import List, Dict
import logging

import boto3
from botocore.config import Config
from fastapi import APIRouter, HTTPException, status
from psycopg_pool import ConnectionPool

from bdi_api.settings import DBCredentials, Settings

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize settings and credentials
settings = Settings()
db_credentials = DBCredentials()

# Configure S3 client
s3_client = boto3.client(
    "s3",
    config=Config(retries={"max_attempts": 5}, connect_timeout=10, read_timeout=30)
)

# Set up PostgreSQL connection pool
pool = ConnectionPool(
    conninfo=f"""
        dbname=postgres
        user={db_credentials.username}
        password={db_credentials.password}
        host={db_credentials.host}
        port={db_credentials.port}
    """,
    max_size=20,
    max_lifetime=600,
    timeout=10
)

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

def process_aircraft_data(data: dict) -> List[tuple]:
    """Process raw aircraft data into a list of tuples for database insertion."""
    aircraft_list = []
    timestamp = data.get("now", 0)
    for ac in data.get("aircraft", []):
        altitude_baro = ac.get("alt_baro", 0)
        if altitude_baro == "ground":
            altitude_baro = 0
        emergency = ac.get("emergency", "none")
        had_emergency = emergency not in ["none", None]
        aircraft_list.append((
            ac.get("hex"), ac.get("r"), ac.get("t"), ac.get("lat"), ac.get("lon"),
            ac.get("gs", 0), altitude_baro, timestamp, had_emergency
        ))
    return aircraft_list

@s7.post("/aircraft/prepare")
async def prepare_data() -> str:
    """Fetch raw data from S3 and insert it into PostgreSQL on AWS RDS."""
    s3_prefix = "raw/day=20231101/"
    total_records = 0
    logger.info(f"Starting data preparation from S3 prefix: {s3_prefix}")

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=settings.s3_bucket, Prefix=s3_prefix)

        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS aircraft (
                        icao VARCHAR(7), registration VARCHAR(10), type VARCHAR(10),
                        lat FLOAT, lon FLOAT, ground_speed FLOAT, altitude_baro FLOAT,
                        timestamp FLOAT, had_emergency BOOLEAN
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft (icao)")
                logger.info("Aircraft table and index created or verified")

                for page in pages:
                    for obj in page.get("Contents", []):
                        logger.info(f"Processing S3 object: {obj['Key']}")
                        response = s3_client.get_object(Bucket=settings.s3_bucket, Key=obj["Key"])
                        content = response["Body"].read()

                        try:
                            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz_file:
                                data = json.load(gz_file)
                        except (OSError, json.JSONDecodeError):
                            data = json.loads(content.decode("utf-8"))

                        aircraft_data = process_aircraft_data(data)
                        if aircraft_data:
                            insert_query = """
                                INSERT INTO aircraft (icao, registration, type, lat, lon, ground_speed, altitude_baro, timestamp, had_emergency)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (icao, timestamp) DO NOTHING
                            """
                            cursor.executemany(insert_query, aircraft_data)
                            total_records += len(aircraft_data)
                            logger.info(f"Inserted {len(aircraft_data)} records from {obj['Key']}")

                conn.commit()
                logger.info(f"Total records inserted: {total_records}")

        return f"Successfully inserted {total_records} records into the database"

    except Exception as e:
        logger.error(f"Data preparation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data preparation failed: {str(e)}")

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> List[Dict]:
    """List distinct aircraft with their registration and type, ordered by ICAO."""
    logger.info(f"Listing aircraft: num_results={num_results}, page={page}")
    query = """
        SELECT DISTINCT icao, registration, type
        FROM aircraft
        ORDER BY icao ASC
        LIMIT %s OFFSET %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (num_results, page * num_results))
            results = cursor.fetchall()
    logger.info(f"Retrieved {len(results)} aircraft")
    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in results]

@s7.get("/aircraft/{icao}/positions")
def get_aircraft_positions(icao: str, num_results: int = 1000, page: int = 0) -> List[Dict]:
    """Retrieve all known positions for a specific aircraft, ordered by timestamp."""
    logger.info(f"Fetching positions for ICAO {icao}: num_results={num_results}, page={page}")
    query = """
        SELECT timestamp, lat, lon
        FROM aircraft
        WHERE icao = %s
        AND lat IS NOT NULL
        AND lon IS NOT NULL
        ORDER BY timestamp ASC
        LIMIT %s OFFSET %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (icao, num_results, page * num_results))
            results = cursor.fetchall()
    logger.info(f"Retrieved {len(results)} positions for ICAO {icao}")
    return [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in results]

@s7.get("/aircraft/{icao}/stats")
def get_aircraft_stats(icao: str) -> Dict:
    """Retrieve statistics for a specific aircraft."""
    logger.info(f"Fetching stats for ICAO {icao}")
    query = """
        SELECT 
            COALESCE(MAX(altitude_baro), 0) AS max_altitude_baro,
            COALESCE(MAX(ground_speed), 0) AS max_ground_speed,
            COALESCE(BOOL_OR(had_emergency), FALSE) AS had_emergency
        FROM aircraft
        WHERE icao = %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (icao,))
            result = cursor.fetchone()
    stats = {"max_altitude_baro": result[0], "max_ground_speed": result[1], "had_emergency": result[2]} if result else {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}
    logger.info(f"Stats for ICAO {icao}: {stats}")
    return stats

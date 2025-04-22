from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import gzip
import io
import boto3
from psycopg_pool import ConnectionPool
from bdi_api.settings import DBCredentials, Settings
import os

# Initialize settings and credentials
settings = Settings()
db_credentials = DBCredentials()

# S3 client setup for MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# PostgreSQL connection pool
pool = ConnectionPool(
    conninfo=f"""
        dbname=postgres
        user={db_credentials.username}
        password={db_credentials.password}
        host={db_credentials.host}
        port={db_credentials.port}
    """,
    min_size=4,
    max_size=50,  # Increased from 20 to 50
    max_lifetime=600,
    timeout=30,  # Increased from 10 to 30 seconds
)

# Check and repair pool connections
def check_pool_connections():
    try:
        pool.check()  # Verify and repair stale connections
    except Exception as e:
        print(f"Pool check failed: {e}")

def process_aircraft_data(data: dict) -> list:
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

# DAG 1: readsb-hist data
def download_readsb_hist_data(**context):
    execution_date = context['execution_date'].replace(day=1)
    day_str = execution_date.strftime('%Y/%m/%d')
    base_url = f"https://samples.adsbexchange.com/readsb-hist/{day_str}/"
    for i in range(100):  # Limit to 100 files
        file_url = f"{base_url}{i:06d}Z.json.gz"
        response = requests.get(file_url)
        if response.status_code == 200:
            s3_client.put_object(
                Bucket=settings.s3_bucket,
                Key=f"raw/day={day_str}/{i:06d}Z.json.gz",
                Body=response.content
            )

def prepare_readsb_hist_data(**context):
    execution_date = context['execution_date'].replace(day=1)
    day_str = execution_date.strftime('%Y/%m/%d')
    total_records = 0
    check_pool_connections()  # Check connections before task
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aircraft (
                    icao VARCHAR(7), registration VARCHAR(10), type VARCHAR(10),
                    lat FLOAT, lon FLOAT, ground_speed FLOAT, altitude_baro FLOAT,
                    timestamp FLOAT, had_emergency BOOLEAN,
                    CONSTRAINT aircraft_unique UNIQUE (icao, timestamp)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft (icao)")
            for i in range(100):
                try:
                    obj = s3_client.get_object(
                        Bucket=settings.s3_bucket,
                        Key=f"raw/day={day_str}/{i:06d}Z.json.gz"
                    )
                    with gzip.GzipFile(fileobj=io.BytesIO(obj['Body'].read())) as gz_file:
                        data = json.load(gz_file)
                    aircraft_data = process_aircraft_data(data)
                    if aircraft_data:
                        cursor.executemany("""
                            INSERT INTO aircraft (icao, registration, type, lat, lon, ground_speed, altitude_baro, timestamp, had_emergency)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT aircraft_unique DO NOTHING
                        """, aircraft_data)
                        total_records += len(aircraft_data)
                except Exception as e:
                    print(f"Error processing file {i:06d}Z.json.gz: {e}")
            conn.commit()
    print(f"Inserted {total_records} records for {day_str}")

with DAG(
    'readsb_hist_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='@monthly',
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
) as dag:
    download_task = PythonOperator(
        task_id='download_readsb_hist',
        python_callable=download_readsb_hist_data,
    )
    prepare_task = PythonOperator(
        task_id='prepare_readsb_hist',
        python_callable=prepare_readsb_hist_data,
    )
    download_task >> prepare_task

# DAG 2: aircraft_type_fuel_consumption_rates
def download_fuel_consumption_data():
    url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    response = requests.get(url)
    data = response.json()
    s3_client.put_object(
        Bucket=settings.s3_bucket,
        Key="fuel_consumption/aircraft_type_fuel_consumption_rates.json",
        Body=json.dumps(data)
    )

with DAG(
    'fuel_consumption_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='@once',
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:
    download_fuel_task = PythonOperator(
        task_id='download_fuel_consumption',
        python_callable=download_fuel_consumption_data,
    )

# DAG 3: Aircraft Database
def download_aircraft_database():
    check_pool_connections()  # Check connections before task
    url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
    response = requests.get(url)
    data = response.text
    s3_client.put_object(
        Bucket=settings.s3_bucket,
        Key="aircraft_database/aircraftDatabase.csv",
        Body=data.encode('utf-8')
    )
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aircraft_database (
                    icao VARCHAR(7) PRIMARY KEY,
                    registration VARCHAR(20),
                    manufacturer VARCHAR(100),
                    model VARCHAR(100),
                    ownop VARCHAR(100)
                )
            """)
            cursor.execute("TRUNCATE TABLE aircraft_database")  # Full refresh
            for line in data.splitlines()[1:]:  # Skip header
                cols = line.split(',')
                if len(cols) >= 5:
                    cursor.execute("""
                        INSERT INTO aircraft_database (icao, registration, manufacturer, model, ownop)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (icao) DO UPDATE SET
                            registration = EXCLUDED.registration,
                            manufacturer = EXCLUDED.manufacturer,
                            model = EXCLUDED.model,
                            ownop = EXCLUDED.ownop
                    """, (cols[0], cols[1], cols[2], cols[3], cols[4]))
            conn.commit()

with DAG(
    'aircraft_database_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='@weekly',
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:
    download_aircraft_db_task = PythonOperator(
        task_id='download_aircraft_database',
        python_callable=download_aircraft_database,
    )
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
import boto3
from bdi_api.main import app
from bdi_api.settings import Settings

client = TestClient(app)
test_settings = Settings(
    s3_bucket="test-bucket",
    raw_dir="tests/s4_data/raw",
    prepared_dir="tests/s4_data/prepared"
)

@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(scope="module")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")

@pytest.fixture(scope="module")
def setup_s3_bucket(s3):
    s3.create_bucket(Bucket=test_settings.s3_bucket)
    yield

def test_s4_download_endpoint(setup_s3_bucket):
    # Override settings for test
    app.dependency_overrides[Settings] = lambda: test_settings
    
    response = client.post(
        "/api/s4/aircraft/download",
        params={"file_limit": 5}
    )
    assert response.status_code == 200
    assert "stored" in response.text.lower()

def test_s4_prepare_endpoint(setup_s3_bucket):
    app.dependency_overrides[Settings] = lambda: test_settings
    
    response = client.post("/api/s4/aircraft/prepare")
    assert response.status_code == 200
    assert "parquet" in response.text.lower()
    
    # Verify local file creation
    prepared_file = os.path.join(test_settings.prepared_dir, "day=20231101", "aircraft_data.parquet")
    assert os.path.exists(prepared_file)

def test_s4_query_endpoints():
    app.dependency_overrides[Settings] = lambda: test_settings
    
    # Test list endpoint
    response = client.get("/api/s4/aircraft/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

    # Test positions endpoint
    response = client.get("/api/s4/aircraft/ABCDEF/positions")
    assert response.status_code == 200

    # Test stats endpoint
    response = client.get("/api/s4/aircraft/ABCDEF/stats")
    assert response.status_code == 200
    assert "max_altitude_baro" in response.json()

# Cleanup
def teardown_module():
    import shutil
    shutil.rmtree("tests/s4_data", ignore_errors=True)
    app.dependency_overrides.clear()

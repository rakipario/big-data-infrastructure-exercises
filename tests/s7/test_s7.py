import time
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock

from bdi_api.s7.exercise import s7

client = TestClient(s7)

class TestS7Student:
    """Test suite for S7 endpoints."""

    @patch('bdi_api.s7.exercise.s3_client')
    @patch('bdi_api.s7.exercise.pool')
    def test_unit_prepare(self, mock_pool, mock_s3_client):
        """Test the /aircraft/prepare endpoint."""
        # Mock S3 response
        mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "raw/day=20231101/file1.json.gz"}]}
        ]
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b'{"now": 123, "aircraft": [{"hex": "06a0af", "emergency": "squawk"}]}')
        }
        # Mock database cursor
        mock_cursor = Mock()
        mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        response = client.post("/api/s7/aircraft/prepare")
        assert response.status_code == 200
        assert "Successfully inserted" in response.text
        assert "1 records" in response.text  # One record processed

    def test_unit_aircraft(self):
        """Test the /aircraft/ endpoint."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("06a0af", "A7-BAC", "B77W")]
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            response = client.get("/api/s7/aircraft?num_results=1")
            assert response.status_code == 200
            assert response.json() == [{"icao": "06a0af", "registration": "A7-BAC", "type": "B77W"}]

    def test_unit_positions(self):
        """Test the /aircraft/{icao}/positions endpoint with valid ICAO."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [(1609275898.6, 30.404617, -86.476566)]
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            response = client.get("/api/s7/aircraft/06a0af/positions?num_results=1")
            assert response.status_code == 200
            assert response.json() == [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]

    def test_unit_positions_invalid_icao(self):
        """Test the /aircraft/{icao}/positions endpoint with invalid ICAO."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = []
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            response = client.get("/api/s7/aircraft/invalid_icao/positions")
            assert response.status_code == 200
            assert response.json() == []  # Empty list for invalid ICAO

    def test_unit_stats(self):
        """Test the /aircraft/{icao}/stats endpoint with valid ICAO."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (30000, 493, True)
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            response = client.get("/api/s7/aircraft/06a0af/stats")
            assert response.status_code == 200
            assert response.json() == {"max_altitude_baro": 30000, "max_ground_speed": 493, "had_emergency": True}

    def test_unit_stats_invalid_icao(self):
        """Test the /aircraft/{icao}/stats endpoint with invalid ICAO."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = None
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            response = client.get("/api/s7/aircraft/invalid_icao/stats")
            assert response.status_code == 200
            assert response.json() == {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

    def test_unit_stats_20ms(self):
        """Test that the /aircraft/{icao}/stats endpoint responds within 20ms."""
        with patch('bdi_api.s7.exercise.pool') as mock_pool:
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (30000, 493, True)
            mock_pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            start_time = time.time()
            response = client.get("/api/s7/aircraft/06a0af/stats")
            elapsed_time = (time.time() - start_time) * 1000  # Convert to ms
            print(f"Elapsed time: {elapsed_time:.2f} ms")
            assert elapsed_time < 20, "Request took too long"
            assert response.status_code == 200
            assert isinstance(response.json(), dict)
          
    def test_integration(self):
        """Integration test running all endpoints."""
        self.test_unit_prepare()
        self.test_unit_aircraft()
        self.test_unit_positions()
        self.test_unit_stats()

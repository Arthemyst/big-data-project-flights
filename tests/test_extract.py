import json

import boto3
import pytest
from moto import mock_aws
from unittest.mock import MagicMock, patch
from src.extract import fetch_flight_data, save_to_s3

MOCK_FLIGHT_DATA = {
    "time": 1742988621,
    "states": [
        [
            "408127",
            "MUK7125 ",
            "United Kingdom",
            1742988620, 1742988621, -4.4465, 53.4341, 8237.22, "false", 223.93, 92.11, 0, "null", 8427.72,
            "7740",
            "false", 0],
        [
            "ab1644",
            "UAL1610 ",
            "United States",
            1742988593, 1742988593, -74.1676, 40.6915, "null", "true", 7.72, 205.31, "null", "null", "null",
            "1405",
            "false", 0]
    ]
}


@pytest.fixture
def mock_flights_api_requests_get():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = MOCK_FLIGHT_DATA
    with patch("requests.get", return_value=mock_response) as mock_get:
        yield mock_get


def test_fetch_flight_data_success(mock_flights_api_requests_get):
    data = fetch_flight_data()

    assert data is not None
    assert "time" in data
    assert "states" in data
    assert isinstance(data["states"], list)
    assert len(data["states"]) == 2
    assert data["states"][0][1].strip() == "MUK7125"
    assert data["states"][1][1].strip() == "UAL1610"


@mock_aws
def test_save_to_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)
    save_to_s3(MOCK_FLIGHT_DATA)

    objects = s3.list_objects(Bucket=bucket_name)
    assert "Contents" in objects
    assert len(objects["Contents"]) == 1

    file_key = objects["Contents"][0]["Key"]
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = json.loads(obj["Body"].read().decode("utf-8"))

    assert file_content == MOCK_FLIGHT_DATA

import json
import os
import pytest
from unittest.mock import patch

from src.extract import DataGenerator


@pytest.fixture
def mock_flight_data():
    return {
        "time": 1743160041,
        "states": [
            [
                "3ffc29",
                "DMFSS   ",
                "Germany",
                1743159863, 1743159872, 12.3882, 48.2424, 716.28, False, 37.06, 346.35, -0.33, None, None, "7000",
                False, 0
            ]
        ]
    }


@patch("src.extract.requests.get")
def test_fetch_flight_data_success(mock_requests_get, mock_flight_data):
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = mock_flight_data

    data = DataGenerator.fetch_flight_data()

    assert data is not None
    assert isinstance(data, dict)
    assert "time" in data
    assert "states" in data


@patch("src.extract.requests.get")
def test_fetch_flight_data_failure(mock_requests_get):
    mock_requests_get.return_value.status_code = 500

    data = DataGenerator.fetch_flight_data()

    assert data is None


def test_save_data_to_file(mock_flight_data, tmp_path):
    test_filename = tmp_path / "test_flight_data.json"

    assert not test_filename.exists()

    filename = DataGenerator.save_data_to_file(mock_flight_data)

    assert os.path.exists(filename)

    with open(filename, "r", encoding="utf-8") as file:
        saved_data = json.load(file)

    assert isinstance(saved_data, list)
    assert len(saved_data) > 0
    assert saved_data[0]["time"] == mock_flight_data["time"]

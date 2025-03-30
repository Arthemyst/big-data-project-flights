import json
import os
from datetime import datetime
from unittest.mock import patch, mock_open

import pandas as pd
import pytest

from src.analysis import FlighDataAnalysis  # Adjust import path as needed


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "icao24": ["4b1816", "3ffc29", "4b1816", "5a7821", "3ffc29"],
        "callsign": ["EDW403Y", "DMFSS", "EDW403Y", "LHT320", "DMFSS"],
        "origin_country": ["Switzerland", "Germany", "Switzerland", "France", "Germany"],
        "longitude": [14.8345, 12.3866, 14.8500, 2.3522, 13.0000],
        "latitude": [45.7171, 48.2539, 45.8000, 48.8566, 48.0000],
        "altitude_m": [11582.4, 678.18, 11600.0, 12000.0, 900.0],
        "velocity_m_s": [225.2, 46.2, 230.0, 250.0, 50.0],
        "heading": [314.17, 172.32, 310.00, 300.00, 180.00],
        "speed_kmh": [810.72, 166.32, 828.00, 900.00, 180.00]
    })


@pytest.fixture
def mock_processed_file(tmp_path, sample_dataframe):
    file_path = tmp_path / "processed_data.csv"
    sample_dataframe.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture
def mock_analysis_output_path(tmp_path):
    return str(tmp_path)


def test_read_processed_file_locally(mock_processed_file, sample_dataframe):
    df = FlighDataAnalysis.read_processed_file_locally(mock_processed_file)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == list(sample_dataframe.columns)


def test_read_processed_file_locally_invalid():
    df = FlighDataAnalysis.read_processed_file_locally("/invalid/path/to/file.csv")
    assert df is None, "Should return None if the file is invalid."


def test_analyze_data(sample_dataframe):
    analysis_result = FlighDataAnalysis.analyze_data(sample_dataframe)

    assert isinstance(analysis_result, dict)
    assert "unique_aircraft" in analysis_result
    assert "average_speed_kmh" in analysis_result
    assert "top_countries" in analysis_result
    assert "bounding_box" in analysis_result
    assert "fastest_aircraft" in analysis_result

    assert analysis_result["unique_aircraft"] == 3
    assert analysis_result["bounding_box"]["min_latitude"] == 45.7171
    assert analysis_result["bounding_box"]["max_longitude"] == 14.8500
    assert len(analysis_result["fastest_aircraft"]) == 5


def test_save_transformed_data_invalid_path():
    with patch("os.makedirs") as mock_makedirs, patch("builtins.open", side_effect=OSError("Disk error")):
        analysis_result = {"test": "data"}
        FlighDataAnalysis.save_transformed_data(analysis_result, "/invalid/path")

    mock_makedirs.assert_called()


@patch("builtins.open", new_callable=mock_open)
def test_save_transformed_data(mock_file, sample_dataframe, mock_analysis_output_path):
    analysis_result = FlighDataAnalysis.analyze_data(sample_dataframe)
    FlighDataAnalysis.save_transformed_data(analysis_result, mock_analysis_output_path)

    output_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.json"
    output_filepath = os.path.join(mock_analysis_output_path, output_filename)

    mock_file.assert_called_once_with(output_filepath, "w", encoding="utf-8")

    written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
    saved_data = json.loads(written_content)

    assert isinstance(saved_data, dict)
    assert "unique_aircraft" in saved_data
    assert "bounding_box" in saved_data

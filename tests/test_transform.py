import json
import os
from datetime import datetime
from unittest.mock import patch, mock_open
import pytest

import pandas as pd

from src.transform import FlightDataTransformer

MOCK_JSON_DATA = json.dumps([
    {
        "time": 1743258171,
        "states": [
            ["4b1816", "EDW403Y", "Switzerland", 1743325458.0, 1743325458, 14.8345, 45.7171, 11582.4, False, 225.2,
             314.17, -0.33, None, 11590.02, "5643", False, 0],
            ["3ffc29", "DMFSS", "Germany", 1743325454.0, 1743325454, 12.3866, 48.2539, 678.18, False, 46.2, 172.32, -0.33,
             None, None, "7000", False, 0]
        ]
    }
])

@pytest.fixture
def sample_dataframe():
    """Fixture to provide a sample raw DataFrame similar to the JSON structure."""
    return pd.DataFrame([
        ["4b1816", "EDW403Y", "Switzerland", None, None, 14.8345, 45.7171, 11582.4, None, 225.2, 314.17],
        ["3ffc29", "DMFSS", "Germany", None, None, 12.3866, 48.2539, 678.18, None, 46.2, 172.32],
        ["abcd12", None, "USA", None, None, None, None, 5000, None, 100.0, 200.0]  # Missing lat/lon
    ])


@pytest.fixture
def sample_dataframe_2():
    """Fixture to provide a sample transformed DataFrame."""
    return pd.DataFrame({
        "icao24": ["4b1816", "3ffc29"],
        "callsign": ["EDW403Y", "DMFSS"],
        "origin_country": ["Switzerland", "Germany"],
        "longitude": [14.8345, 12.3866],
        "latitude": [45.7171, 48.2539],
        "altitude_m": [11582.4, 678.18],
        "velocity_m_s": [225.2, 46.2],
        "heading": [314.17, 172.32],
        "speed_kmh": [810.72, 166.32]
    })


@patch("builtins.open", new_callable=mock_open, read_data=MOCK_JSON_DATA)
def test_read_raw_data_locally_valid_data(mock_file):
    df = FlightDataTransformer.read_raw_data_locally("dummy_path.json")

    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 2
    assert len(df.columns) == 17


@patch("builtins.open", new_callable=mock_open, read_data="[]")
def test_read_raw_data_locally_empty_data(mock_file):
    df = FlightDataTransformer.read_raw_data_locally("dummy_path.json")

    assert df is None or df.empty


@patch("builtins.open", new_callable=mock_open, read_data="invalid_json")
def test_read_raw_data_locally_invalid_json(mock_file):
    df = FlightDataTransformer.read_raw_data_locally("dummy_path.json")

    assert df is None


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps([{"time": 1743258171}]))
def test_read_raw_data_locally_missing_states(mock_file):
    df = FlightDataTransformer.read_raw_data_locally("dummy_path.json")

    assert df is None or df.empty


def test_transform_data_valid(sample_dataframe):
    transformed_df = FlightDataTransformer.transform_data(sample_dataframe)

    assert transformed_df is not None
    assert isinstance(transformed_df, pd.DataFrame)
    assert "speed_kmh" in transformed_df.columns
    assert transformed_df["speed_kmh"].iloc[0] == pytest.approx(225.2 * 3.6)
    assert transformed_df.shape[1] == 9


def test_transform_data_removes_invalid_rows(sample_dataframe):
    transformed_df = FlightDataTransformer.transform_data(sample_dataframe)

    assert transformed_df.shape[0] == 2


def test_transform_data_velocity_column():
    df = pd.DataFrame([
        ["4b1816", "EDW403Y", "Switzerland", 1743325458.0, 1743325458, 14.8345, 45.7171, 11582.4, False, 225.2,
         314.17, -0.33, None, 11590.02, "5643", False, 0]
    ])
    transformed_df = FlightDataTransformer.transform_data(df)

    assert "speed_kmh" in transformed_df.columns


@pytest.fixture
def mock_output_path(tmp_path):
    return str(tmp_path)


def test_save_transformed_data_valid(sample_dataframe_2, mock_output_path):

    FlightDataTransformer.save_transformed_data(sample_dataframe_2, mock_output_path)

    output_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    output_filepath = os.path.join(mock_output_path, output_filename)

    assert os.path.exists(output_filepath)
    saved_df = pd.read_csv(output_filepath)
    assert not saved_df.empty
    assert "speed_kmh" in saved_df.columns


def test_save_transformed_data_empty_dataframe(mock_output_path):
    empty_df = pd.DataFrame()
    FlightDataTransformer.save_transformed_data(empty_df, mock_output_path)

    output_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    output_filepath = os.path.join(mock_output_path, output_filename)

    assert not os.path.exists(output_filepath)


def test_save_transformed_data_none_dataframe(mock_output_path):
    FlightDataTransformer.save_transformed_data(None, mock_output_path)

    output_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    output_filepath = os.path.join(mock_output_path, output_filename)

    assert not os.path.exists(output_filepath)


@patch("builtins.open", new_callable=mock_open)
def test_save_transformed_data_ioerror(mock_file, sample_dataframe, mock_output_path):
    mock_file.side_effect = IOError("Disk error")

    with patch("os.path.join", return_value="/invalid_path/output.csv"):
        FlightDataTransformer.save_transformed_data(sample_dataframe, "/invalid_path")


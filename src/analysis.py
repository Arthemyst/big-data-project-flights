import os
import json
import boto3
import pandas as pd
from datetime import datetime
from tools.config import CustomEnvironment

PROCESSED_DATA_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'processed_files')
ANALYSIS_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'analysis_files')
BUCKET_NAME = CustomEnvironment.get_aws_s3_bucket()


class FlighDataAnalysis:

    @staticmethod
    def read_processed_file_locally(file_path: str):
        try:
            df = pd.read_csv(file_path)
            return df

        except Exception as e:
            print(f"Error reading local file: {e}")
            return None

    @staticmethod
    def analyze_data(transformed_df: pd.DataFrame) -> dict:
        unique_aircraft = transformed_df["icao24"].nunique()
        avg_speed = transformed_df["speed_kmh"].mean()
        top_countries = transformed_df["origin_country"].value_counts().head(5)

        min_lat, max_lat = transformed_df["latitude"].min(), transformed_df["latitude"].max()
        min_lon, max_lon = transformed_df["longitude"].min(), transformed_df["longitude"].max()

        fastest_aircraft = transformed_df.nlargest(5, "speed_kmh")[["icao24", "callsign", "speed_kmh"]]

        analysis_results = {
            "unique_aircraft": unique_aircraft,
            "average_speed_kmh": avg_speed,
            "top_countries": top_countries.to_dict(),
            "bounding_box": {
                "min_latitude": min_lat,
                "max_latitude": max_lat,
                "min_longitude": min_lon,
                "max_longitude": max_lon,
            },
            "fastest_aircraft": fastest_aircraft.to_dict(orient="records")
        }
        return analysis_results




    @staticmethod
    def save_transformed_data(data: dict, output_path: str):
        if df is None or df.empty:
            print("No data to save.")
            return

        output_filepath = os.path.join(output_path, f"{datetime.utcnow().strftime('%Y-%m-%d')}.json")
        os.makedirs(output_path, exist_ok=True)

        try:
            with open(output_filepath, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)
            print(f"✅ Transformed data saved locally to: {output_filepath}")
        except Exception as e:
            print(f"Error saving JSON file: {e}")


if __name__ == "__main__":
    input_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    input_filepath = os.path.join(PROCESSED_DATA_DIR, input_filename)
    df = FlighDataAnalysis.read_processed_file_locally(input_filepath)
    transformed_df = FlighDataAnalysis.analyze_data(df)
    FlighDataAnalysis.save_transformed_data(transformed_df, ANALYSIS_DIR)

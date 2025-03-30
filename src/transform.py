import os
import json
import boto3
import pandas as pd
from datetime import datetime
from tools.config import CustomEnvironment

RAW_DATA_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'raw_files')
PROCESSED_DATA_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'processed_files')
BUCKET_NAME = CustomEnvironment.get_aws_s3_bucket()


class FlightDataTransformer:

    @staticmethod
    def read_raw_data_locally(file_path: str):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            return pd.DataFrame(data[0]["states"])
        except Exception as e:
            print(f"❌ Error reading local file: {e}")
            return None

    @staticmethod
    def read_raw_data_from_s3(bucket_name: str, input_path: str):
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_path)

        file_list = [obj['Key'] for obj in response.get('Contents', [])]

        if file_list:
            file = file_list[0]
            obj = s3.get_object(Bucket=bucket_name, Key=file)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            return pd.DataFrame(data.get("states", []))
        else:
            print("No files found in the input folder.")
            return None

    @staticmethod
    def transform_data(df):

        columns = ["icao24", "callsign", "origin_country", None, None, "longitude", "latitude", "altitude_m", None,
                   "velocity_m_s", "heading"]
        df = df.iloc[:, :len(columns)]
        df.columns = columns
        df = df.drop(columns=[col for col in df.columns if col is None], errors='ignore')

        df["speed_kmh"] = df["velocity_m_s"] * 3.6
        df = df.dropna(subset=["latitude", "longitude"])

        return df

    @staticmethod
    def save_transformed_data(df, output_path: str):
        if df is None or df.empty:
            print("❌ No data to save.")
            return

        output_filepath = os.path.join(output_path, f"{datetime.utcnow().strftime('%Y-%m-%d')}.csv")

        try:
            df.to_csv(output_filepath)
            print(f"✅ Transformed data saved locally to: {output_filepath}")
        except Exception as e:
            print(f"❌ Error saving JSON file: {e}")


if __name__ == "__main__":
    input_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.json"
    input_filepath = os.path.join(RAW_DATA_DIR, input_filename)
    df = FlightDataTransformer.read_raw_data_locally(input_filepath)
    transformed_df = FlightDataTransformer.transform_data(df)
    print(transformed_df)
    FlightDataTransformer.save_transformed_data(transformed_df, PROCESSED_DATA_DIR)

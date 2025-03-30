import json
import os
from datetime import datetime

import boto3
import requests
from tools.config import CustomEnvironment

AWS_REGION = CustomEnvironment.get_aws_region()
S3_BUCKET_NAME = CustomEnvironment.get_aws_s3_bucket()
DATA_FOLDER = "raw_files"
API_URL = "https://opensky-network.org/api/states/all"
s3_client = boto3.client("s3", region_name=AWS_REGION)


class DataGenerator:

    @staticmethod
    def fetch_flight_data() -> dict or None:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error during data downloading: {response.status_code}")
            return None

    @staticmethod
    def save_data_to_file(data) -> str:
        filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.json"
        data_dir_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), DATA_FOLDER)
        os.makedirs(data_dir_path, exist_ok=True)

        file_path = os.path.join(data_dir_path, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                existing_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        existing_data.append(data)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(existing_data, file, indent=4)

        return filename


class DataUploader:
    @staticmethod
    def upload_to_s3(file_name: str) -> None:
        data_dir_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), DATA_FOLDER)
        file_path = os.path.join(data_dir_path, filename)
        if not os.path.exists(file_path):
            print(f"File {file_name} does not exist! Upload not possible.")
            return

        try:
            s3_client.upload_file(file_path, S3_BUCKET_NAME, f"raw_files/{file_name}")
            print(f"File {file_name} loaded to S3 t directory 'raw_files/'.")

        except Exception as e:
            print(f"Error during loading {file_name} file to S3: {e}")


if __name__ == "__main__":
    data = DataGenerator.fetch_flight_data()
    filename = DataGenerator.save_data_to_file(data)
    DataUploader.upload_to_s3(filename)

import json
import os
from datetime import datetime

import boto3
import requests

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DATA_FOLDER = "raw_files"
API_URL = "https://opensky-network.org/api/states/all"
s3_client = boto3.client("s3", region_name=AWS_REGION)


def fetch_flight_data() -> dict or None:
    response = requests.get(API_URL)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error during data downloading: {response.status_code}")
        return None


def save_to_s3(data, bucket_name="test-bucket"):
    try:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"flights_data_{timestamp}.json"

        json_data = json.dumps(data, indent=2)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data.encode("utf-8"),
            ContentType="application/json"
        )
        print(f"Successfully saved {file_name} to S3 bucket {bucket_name}")
        return file_name

    except Exception as e:
        print(f"Error during saving to S3: {str(e)}")
        return None


if __name__ == "__main__":
    print("Downloading flights data...")
    flight_data = fetch_flight_data()

    if flight_data:
        print("Saving data to S3...")
        save_to_s3(flight_data)

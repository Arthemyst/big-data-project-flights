import json
import os

import boto3
import pandas as pd

s3_client = boto3.client('s3')


def process_file(file_name, bucket_name):
    local_path = '/tmp/' + os.path.basename(file_name)
    print(f"Loading file: {file_name} from S3")
    s3_client.download_file(bucket_name, file_name, local_path)

    with open(local_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    data = pd.DataFrame(data[0]["states"])
    print(f"Analyzing file: {file_name}")

    print(f"Dataframe columns: {data.columns}")

    columns = ["icao24", "callsign", "origin_country", None, None, "longitude", "latitude", "altitude_m", None,
               "velocity_m_s", "heading"]
    data = data.iloc[:, :len(columns)]
    data.columns = columns
    transformed_df = data.drop(columns=[col for col in data.columns if col is None], errors='ignore')

    print(f"Transformed dataframe columns: {transformed_df.columns}")

    transformed_df["speed_kmh"] = transformed_df["velocity_m_s"] * 3.6
    transformed_df = transformed_df.dropna(subset=["latitude", "longitude"])

    output_filename = file_name.split(".")[0]
    output_file = '/tmp/processed_' + os.path.basename(output_filename) + '.csv'
    transformed_df.to_csv(output_file)

    s3_client.upload_file(output_file, bucket_name, f'processed/{os.path.basename(output_file)}')
    print(f"File processed and saved to: processed/{os.path.basename(output_file)}")


def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(f"Bucket: {bucket_name}, File: {file_key}")

    if file_key.endswith('.json'):
        process_file(file_key, bucket_name)
    else:
        print(f"Skipped file: {file_key}, it's not a JSON file.")

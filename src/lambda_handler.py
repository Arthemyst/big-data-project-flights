import json
import os
import boto3
import pandas as pd

s3_client = boto3.client('s3')


def analyze_data(transformed_df):
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

    output_file = "/tmp/analysis_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(analysis_results, f, indent=4)

    return output_file


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

    analysis_file = analyze_data(transformed_df)
    s3_client.upload_file(analysis_file, bucket_name, f'analysis/{os.path.basename(analysis_file)}')


def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(f"Bucket: {bucket_name}, File: {file_key}")

    if file_key.endswith('.json'):
        process_file(file_key, bucket_name)
    else:
        print(f"Skipped file: {file_key}, it's not a JSON file.")

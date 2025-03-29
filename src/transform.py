import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("FlightDataTransformation").master("local[*]").getOrCreate()
RAW_DATA_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'raw_files')
PROCESSED_DATA_DIR = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'processed_files')


class FlightDataTransformer:

    @staticmethod
    def read_json(file_path: str):
        return spark.read.option("multiline", "true").json(file_path)

    @staticmethod
    def transform_data(dataframe):
        if dataframe is None:
            return None
        exploded_df = dataframe.select(explode(col("states")).alias("state"))

        transformed_df = exploded_df.select(
            col("state").getItem(0).alias("icao24"),
            col("state").getItem(1).alias("callsign"),
            col("state").getItem(2).alias("origin_country"),
            col("state").getItem(5).alias("longitude"),
            col("state").getItem(6).alias("latitude"),
            col("state").getItem(7).alias("altitude_m"),
            col("state").getItem(9).alias("velocity_m_s"),
            col("state").getItem(10).alias("heading")
        )

        transformed_df = transformed_df.withColumn("speed_kmh", col("velocity_m_s") * 3.6)
        transformed_df = transformed_df.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))
        return transformed_df

    @staticmethod
    def save_transformed_data(df, output_path: str):
        if df is None:
            print("❌ No data to save.")
            return

        try:
            df.write.mode("overwrite").json(output_path)
            print(f"✅ Transformed data saved to: {output_path}")
        except Exception as e:
            print(f"❌ Error saving JSON file: {e}")


if __name__ == "__main__":
    input_filename = f"{datetime.utcnow().strftime('%Y-%m-%d')}.json"
    input_filepath = os.path.join(RAW_DATA_DIR, input_filename)
    output_filepath = os.path.join(PROCESSED_DATA_DIR, input_filename)
    raw_df = FlightDataTransformer.read_json(input_filepath)
    transformed_df = FlightDataTransformer.transform_data(raw_df)
    FlightDataTransformer.save_transformed_data(transformed_df, output_filepath)

import dlt
import requests
import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Schema for the output
output_schema = StructType([
    StructField("zipcode", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("http_status_code", IntegerType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("processed_date", DateType(), True),
    StructField("processed_hour", IntegerType(), True)
])

@dlt.table(
    comment="Streaming table that fetches raw API JSON for each generated URL in parallel",
    temporary=False
)
def brz_raw_data_scrapper():
    # Read configuration from pipeline settings
    authority = spark.conf.get("authority")

    # Base headers
    base_headers = {
        "authority": authority,
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.7",
        "sec-ch-ua": '"Not A(Brand";v="99", "Brave";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }

    # Read streaming source table with URLs
    urls_df = spark.readStream.table("brz_genereated_urls").select("zip", "api_url", "refereral_url")

    # UDF to fetch API data
    def fetch_api(zipcode, api_url, refereral_url):
        try:
            headers = dict(base_headers)
            headers["referer"] = refereral_url
            resp = requests.get(api_url, headers=headers, timeout=10)
            return Row(
                zipcode=zipcode,
                source_url=api_url,
                raw_json=resp.text,
                http_status_code=resp.status_code,
                ingestion_timestamp=datetime.utcnow(),
                processed_date=datetime.utcnow().date(),
                processed_hour=datetime.utcnow().hour
            )
        except Exception as e:
            return Row(
                zipcode=zipcode,
                source_url=api_url,
                raw_json=json.dumps({"error": str(e)}),
                http_status_code=500,
                ingestion_timestamp=datetime.utcnow(),
                processed_date=datetime.utcnow().date(),
                processed_hour=datetime.utcnow().hour
            )

    fetch_api_udf = F.udf(fetch_api, output_schema)

    # Apply the UDF to the streaming DataFrame
    api_df = urls_df.withColumn("api_data", fetch_api_udf(F.col("zip"), F.col("api_url"), F.col("refereral_url")))

    # Select the required columns from the nested structure
    final_df = api_df.select("api_data.*")

    # Write the DataFrame to a Delta table
    return final_df.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoints/brz_raw_data_scrapper").table("brz_raw_data_scrapper")
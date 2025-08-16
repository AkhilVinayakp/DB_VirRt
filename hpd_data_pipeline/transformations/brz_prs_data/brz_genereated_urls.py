import dlt
from pyspark.sql import functions as F

@dlt.table
def brz_genereated_urls():
    api_url_template = spark.conf.get("api_url")
    referal_url_template = spark.conf.get("referal_url")

    # Stream CSV files from Volume (not checkpoint folder!)
    source_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/mycatalog/hp_prd_data/checkpoint_v/zips")
    )

    return (
        source_df
        .withColumn("api_url", F.format_string(api_url_template, F.col("zip").cast("string")))
        .withColumn("refereral_url", F.format_string(referal_url_template, F.col("zip").cast("string"), F.col("state_id").cast("string"), F.col("state_name")))
        .withColumn("processed_datehour", F.date_format(F.current_timestamp(), "yyyy-MM-dd-HH"))
        .withColumn("batch_id", F.current_timestamp())
    )

import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Streaming table that generates URLs based on zip and state data",
    temporary=False
)
def brz_genereated_urls():
    # Read configuration from DLT pipeline settings
    api_url_template = spark.conf.get("api_url")
    referal_url_template = spark.conf.get("referal_url")
    zip_load_count = int(spark.conf.get("zip_load_count"))

    # Read streaming source
    source_df = (
        spark.readStream
        .table("uszips")
    )

    # Filter, randomize, and limit
    sub_df = (
        source_df
        .filter(F.col("population").isNotNull())
        .withColumn("state_name", F.regexp_replace(F.col("state_name"), " ", "-"))
        .orderBy(F.rand())
        .limit(zip_load_count)
    )

    # Build URLs
    final_df = (
        sub_df
        .withColumn(
            "api_url",
            F.format_string(api_url_template, F.col("zip"))
        )
        .withColumn(
            "refereral_url",
            F.format_string(referal_url_template, F.col("zip"), F.col("state_id"), F.col("state_name"))
        )
        .withColumn(
            "processed_datehour",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd-HH")
        )
    )

    return final_df

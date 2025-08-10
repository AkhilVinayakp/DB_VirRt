import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Deduplicated home property data from nearby_parsed_data stream",
    temporary=False
)
def feat_ref_dump():
    source_df = (
        spark.readStream
        .table("brz_raw_parsed")
    )

    # Compute derived columns
    parsed_df = (
        source_df
        .withColumnRenamed("mls#", "house_mls")
        .withColumn("hoa", F.coalesce(F.col("hoa"), F.lit("0")))
        .withColumn("age", F.year(F.current_date()) - F.col("yearBuilt"))
    )

    # Filter out invalid zip codes
    parsed_df = parsed_df.filter(F.col("zip").cast("double").isNotNull())

    # Deduplicate using dropDuplicates with event-time column
    dedup_df = parsed_df.dropDuplicates(["propertyId"])

    return dedup_df
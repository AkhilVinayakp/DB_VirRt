import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Parsed home data enriched with location details from uszips",
    temporary=False
)
def fp_parsed_data():
    # Read streaming deduplicated home data
    parsed_data_df = spark.readStream.table("mycatalog.hp_prd_data.feat_ref_dump")

    # Read static location data (uszips is not streaming, so batch read is fine)
    location_data_df = (
        spark.read.table("mycatalog.hp_prd_data.uszips")
        .select(
            F.col("zip"),
            F.col("lat").alias("zip_lat"),
            F.col("lng").alias("zip_lng"),
            F.col("population").alias("zip_population"),
            F.col("density").alias("zip_density")
        )
    )

    # Join on zip
    enriched_df = (
        parsed_data_df
        .join(location_data_df, on="zip", how="inner")
    )

    return enriched_df

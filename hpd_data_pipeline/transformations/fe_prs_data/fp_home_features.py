import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Home features extracted from listingRemarks using databricks-gemma-3-12b",
    temporary=False
)
def fp_home_features():
    # Read the enriched parsed home data
    parsed_df = spark.readStream.table("mycatalog.hp_prd_data.feat_ref_dump")

    # Define the LLM prompt template
    example_text = (
        "Extract the top 4 key features from the home description below as a comma-separated list. "
        "Example:\n"
        "Input: Very nice home with Spacious floor plan, Multiple living areas, Desirable location, Fenced and gated property. "
        "Output should be: Spacious floor plan, Multiple living areas, Desirable location, Fenced and gated property\n"
    )

    # Apply ai_query to listingRemarks
    features_df = parsed_df.select(
        "propertyId",
        "listingRemarks",
        F.expr(
            "ai_query('databricks-gemma-3-12b', concat('{0}', listingRemarks))".format(example_text)
        ).alias("top_features")
    )

    return features_df

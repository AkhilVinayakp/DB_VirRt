import dlt
import json
from pyspark.sql.functions import udf, explode, current_timestamp
from pyspark.sql.types import *

# Define schema for flattened home records
home_schema = StructType([
    StructField("search_zipcode", StringType()),
    StructField("mls#", StringType()),
    StructField("mls_status", StringType()),
    StructField("house_price", LongType()),
    StructField("hoa", StringType()),
    StructField("sqft", StringType()),
    StructField("price_per_sqft", StringType()),
    StructField("lot_size", StringType()),
    StructField("beds", StringType()),
    StructField("baths", StringType()),
    StructField("fullbaths", StringType()),
    StructField("partialBaths", StringType()),
    StructField("location", StringType()),
    StructField("stories", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("streetLine", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", StringType()),
    StructField("postalCode", StringType()),
    StructField("countryCode", StringType()),
    StructField("searchStatus", IntegerType()),
    StructField("propertyType", IntegerType()),
    StructField("uiPropertyType", IntegerType()),
    StructField("listingType", IntegerType()),
    StructField("propertyId", LongType()),
    StructField("listingId", LongType()),
    StructField("dataSourceId", IntegerType()),
    StructField("marketId", IntegerType()),
    StructField("yearBuilt", IntegerType()),
    StructField("openHouseStartFormatted", StringType()),
    StructField("openHouseEventName", StringType()),
    StructField("url", StringType()),
    StructField("isHot", BooleanType()),
    StructField("hasVirtualTour", BooleanType()),
    StructField("hasVideoTour", BooleanType()),
    StructField("has3DTour", BooleanType()),
    StructField("isActiveKeyListing", BooleanType()),
    StructField("isNewConstruction", BooleanType()),
    StructField("listingRemarks", StringType()),
    StructField("scanUrl", StringType()),
    StructField("posterFrameUrl", StringType())
])

# UDF to parse and flatten home listings
def extract_listings(text, search_zipcode):
    results = []
    try:
        json_text = text[4:]
        data = json.loads(json_text)
        homes = data.get("payload", {}).get("homes", [])
        for row in homes:
            try:
                results.append({
                    "search_zipcode": search_zipcode,
                    "mls#": row.get("mlsId", {}).get("value"),
                    "mls_status": row.get("mlsStatus"),
                    "house_price": row.get("price", {}).get("value"),
                    "hoa": row.get("hoa", {}).get("value") if "hoa" in row and "value" in row["hoa"] else None,
                    "sqft": row.get("sqFt", {}).get("value") if "sqFt" in row else None,
                    "price_per_sqft": row.get("pricePerSqFt", {}).get("value"),
                    "lot_size": row.get("lotSize", {}).get("value"),
                    "beds": row.get("beds"),
                    "baths": row.get("baths"),
                    "fullbaths": row.get("fullbaths"),
                    "partialBaths": row.get("partialBaths"),
                    "location": row.get("location", {}).get("value"),
                    "stories": row.get("stories"),
                    "latitude": row.get("latLong", {}).get("value", {}).get("latitude"),
                    "longitude": row.get("latLong", {}).get("value", {}).get("longitude"),
                    "streetLine": row.get("streetLine", {}).get("value"),
                    "city": row.get("city"),
                    "state": row.get("state"),
                    "zip": row.get("zip"),
                    "postalCode": row.get("postalCode", {}).get("value"),
                    "countryCode": row.get("countryCode"),
                    "searchStatus": row.get("searchStatus"),
                    "propertyType": row.get("propertyType"),
                    "uiPropertyType": row.get("uiPropertyType"),
                    "listingType": row.get("listingType"),
                    "propertyId": row.get("propertyId"),
                    "listingId": row.get("listingId"),
                    "dataSourceId": row.get("dataSourceId"),
                    "marketId": row.get("marketId"),
                    "yearBuilt": row.get("yearBuilt", {}).get("value"),
                    "openHouseStartFormatted": row.get("openHouseStartFormatted"),
                    "openHouseEventName": row.get("openHouseEventName"),
                    "url": row.get("url"),
                    "isHot": row.get("isHot"),
                    "hasVirtualTour": row.get("hasVirtualTour"),
                    "hasVideoTour": row.get("hasVideoTour"),
                    "has3DTour": row.get("has3DTour"),
                    "isActiveKeyListing": row.get("isActiveKeyListing"),
                    "isNewConstruction": row.get("isNewConstruction"),
                    "listingRemarks": row.get("listingRemarks"),
                    "scanUrl": row.get("scanUrl"),
                    "posterFrameUrl": row.get("posterFrameUrl")
                })
            except:
                continue
    except:
        pass
    return results

extract_udf = udf(extract_listings, ArrayType(home_schema))



# Step 2: Apply parsing logic and flatten JSON
@dlt.table(
    comment="Parsed nearby homes data"
)
def brz_raw_parsed():
    source_df = spark.readStream.format("delta").table("brz_raw_data_scrapper")
    df_extracted = source_df.withColumn(
        "homes_array", extract_udf("raw_json", "zipcode")
    )
    final_df = df_extracted.select(explode("homes_array").alias("home")).select("home.*")
    final_df = final_df.withColumn("ingestion_timestamp", current_timestamp())
    return final_df

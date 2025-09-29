# Pushing the data to pinecode for semantic search

from pyspark.sql.functions import udf, col, when, coalesce, lit
from pyspark.sql.types import StringType, Row
from pinecone import Pinecone
import time

def create_template(propertyType, location, city, state, countryCode, postalCode,
                   yearBuilt, sqft, lot_size, beds, baths, fullbaths, partialBaths,
                   house_price, stories, top_features, zip_population, zip_density):
    """
    Create a template string for property data with null handling
    """
    # Helper function to handle null values
    def safe_str(value, default="N/A"):
        return str(value) if value is not None else default
    
    return (
        f"Property Type: {safe_str(propertyType)}. "
        f"Located in {safe_str(location)}, {safe_str(city)}, {safe_str(state)}, {safe_str(countryCode)}, "
        f"ZIP {safe_str(postalCode)}. "
        f"Built in {safe_str(yearBuilt)}, approximately {safe_str(sqft)} sqft "
        f"on a {safe_str(lot_size)} sqft lot with {safe_str(beds)} bedrooms and {safe_str(baths)} bathrooms "
        f"({safe_str(fullbaths)} full, {safe_str(partialBaths)} partial). "
        f"Listed price is ${safe_str(house_price)}. "
        f"Stories: {safe_str(stories)}. "
        f"Additional Features: {safe_str(top_features)}. "
        f"Neighborhood population: {safe_str(zip_population)}, density: {safe_str(zip_density)}."
    )

# Register UDF with proper parameter mapping
template_udf = udf(create_template, StringType())

# Load data with join
df = spark.sql(f"""
-- Get max timestamp if available, otherwise based on config
WITH ctrl AS (
  SELECT MAX(last_processed_ts) AS max_ts FROM mycatalog.hp_prd_data.fp_vector_data_ctrl_table
)
SELECT
    fp_parsed.*,
    fp_hm_feat.top_features
FROM mycatalog.hp_prd_data.fp_parsed_data fp_parsed
INNER JOIN mycatalog.hp_prd_data.fp_home_features fp_hm_feat
ON fp_parsed.propertyId = fp_hm_feat.propertyId
WHERE ingestion_timestamp > COALESCE((SELECT max_ts FROM ctrl), date_sub(current_date(), spark.conf.get("max_days_back")))
""")

# Define columns needed for embedding
columns_needed = [
    'propertyId', 'ingestion_timestamp',
    'propertyType', 'location', 'city', 'state', 'countryCode', 'postalCode',
    'yearBuilt', 'sqft', 'lot_size', 'beds', 'baths', 'fullbaths', 'partialBaths',
    'house_price', 'stories', 'top_features', 'zip_population', 'zip_density'
]

# Select required columns and create embedding text
df_text = df.select(*columns_needed)

# Apply UDF with individual column references
df_text = df_text.withColumn(
    "embedding_text", 
    template_udf(
        col('propertyType'), col('location'), col('city'), col('state'), 
        col('countryCode'), col('postalCode'), col('yearBuilt'), col('sqft'),
        col('lot_size'), col('beds'), col('baths'), col('fullbaths'), 
        col('partialBaths'), col('house_price'), col('stories'), 
        col('top_features'), col('zip_population'), col('zip_density')
    )
)
df_text.createOrReplaceTempView("property_text")


# Pushing the data to pinecone

pc = Pinecone(api_key=spark.conf.get("pinecone_api_key")) # Key from env variable.

index_name = "hp-prd-vector-db" # need to parameterize
if not pc.has_index(index_name):
    pc.create_index_for_model(
        name=index_name,
        cloud="aws",
        region="us-east-1",
        embed={
            "model":"llama-text-embed-v2",
            "field_map":{"text": "embedding_text"}
        }
    )
# creating the dense index.
dense_index = pc.Index(index_name)

records = spark.sql("SELECT cast(propertyId as STRING) as _id, embedding_text FROM property_text LIMIT 20").toPandas().to_dict(orient="records")
dense_index.upsert_records("example-namespace", records)

max_iter_wait = 0
while True:
    stats = dense_index.describe_index_stats()
    if stats.total_vector_count >= 0:
        # update the control table and break.
        spark.sql(f"""
            INSERT INTO mycatalog.hp_prd_data.fp_vector_data_ctrl_table
            (
            last_processed_ts,
            last_pushed_ts
            )
            select current_timestamp(),
            (select max(ingestion_timestamp) from property_text)
            """)
        break
    if max_iter_wait > 5:
        break
    print("Waiting for vectors to be indexed...")
    max_iter_wait += 1
    time.sleep(5) 


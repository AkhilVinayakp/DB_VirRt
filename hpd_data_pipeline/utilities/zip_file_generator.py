from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime

spark = SparkSession.builder.getOrCreate()

def export_zipcodes_to_volume(output_path: str, count: int = 100):
    """
    Picks random `count` zipcodes from uszips table and writes as CSV file into a DBFS volume path.
    
    Args:
        output_path (str): Path in DBFS where the CSV will be written (e.g., '/mnt/vol/uszips_input/').
        count (int): Number of zipcodes to sample.
    """
    # Step 1: Read static uszips table
    df = spark.read.table("mycatalog.hp_prd_data.uszips")
    
    # Step 2: Sample zipcodes
    sampled_df = (
        df.filter(F.col("population").isNotNull())
          .orderBy(F.rand())
          .limit(count)
          .select("zip", "state_id", "state_name", "population")
    )
    
    # Step 3: Add a batch_id for traceability
    batch_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    sampled_df = sampled_df.withColumn("batch_id", F.lit(batch_id))
    
    # Step 4: Write as CSV into DBFS volume (each run creates a new file)
    (
        sampled_df.coalesce(1)  # write as a single file
        .write
        .mode("overwrite")      # overwrite only within this batch folder
        .option("header", "true")
        .csv(f"{output_path}/zips/batch_{batch_id}")
    )
    
    print(f"✅ Exported {count} zipcodes to {output_path}/batch_{batch_id}")

# Example usage:
export_zipcodes_to_volume("/Volumes/mycatalog/hp_prd_data/checkpoint_v", count=100)

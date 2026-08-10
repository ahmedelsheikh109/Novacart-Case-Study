from pyspark.sql.functions import col, current_timestamp, max as spark_max, lit
from delta.tables import DeltaTable
from datetime import datetime
import uuid

# ==========================================
# (Configurations)
# ==========================================
spark.sql("USE CATALOG work")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_schema")

control_table = "work.bronze_schema.ingestion_control"

# Create simple control table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {control_table} (
        table_name STRING,
        last_successful_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

tables_config = {
    "orders": "updated_at",
    "products": "updated_at",
    "payments": "processed_at",
}

bronze_run_id = str(uuid.uuid4())
print(f"Current Bronze Run ID: {bronze_run_id}")

# ==========================================
# (State Management)
# ==========================================
def get_last_processed_ts(table_name):
    if spark.catalog.tableExists(control_table):
        try:
            return spark.table(control_table).filter(col("table_name") == table_name).select(spark_max("last_successful_ts")).collect()[0][0]
        except:
            return None
    return None

# ==========================================
# (Incremental Fetch & Load)
# ==========================================
for table_name, ts_col in tables_config.items():
    source_table = f"sql_connection_catalog.dbo.{table_name}"
    target_table = f"work.bronze_schema.{table_name}_raw"
    
    last_ts = get_last_processed_ts(table_name)
    print(f"\n=== Processing {table_name} ===")
    print(f"Last successful ts: {last_ts}")

    source_df = spark.read.table(source_table).withColumn(ts_col, col(ts_col).cast("timestamp"))
    
    df_new = source_df.filter(col(ts_col) > lit(last_ts)) if last_ts else source_df
    
    rows_count = df_new.count()
    print(f"{table_name} new rows found: {rows_count}")

    if rows_count > 0:
        df_new = (
            df_new
            .withColumn("bronze_ingested_at", current_timestamp())
            .withColumn("bronze_run_id", lit(bronze_run_id))
            .withColumn("bronze_source_table", lit(source_table))
        )
        
        df_new.write.format("delta").mode("append").saveAsTable(target_table)
        
        max_ts = df_new.agg(spark_max(ts_col)).collect()[0][0]
        
        # Update State via MERGE
        control_df = spark.createDataFrame(
            [(table_name, max_ts, datetime.now())],
            ["table_name", "last_successful_ts", "updated_at"]
        ).withColumn("last_successful_ts", col("last_successful_ts").cast("timestamp")).withColumn("updated_at", col("updated_at").cast("timestamp"))
        
        dt = DeltaTable.forName(spark, control_table)
        (
            dt.alias("t").merge(control_df.alias("s"), "t.table_name = s.table_name")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        )
        
        print(f"Wrote {rows_count} rows to {target_table} and updated watermark.")
    else:
        print("No new records.")

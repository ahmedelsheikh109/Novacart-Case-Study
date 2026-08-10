from pyspark.sql.functions import col, lit, upper, trim, regexp_replace, when, to_timestamp, to_date, year, month, dayofmonth, date_format, current_timestamp, row_number, max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import uuid

# ==========================================
# (Configurations)
# ==========================================
spark.sql("USE CATALOG work")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_schema")

control_table = "work.silver_schema.processing_control"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {control_table} (
        table_name STRING,
        last_processed_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

silver_run_id = str(uuid.uuid4())
print(f"Current Silver Run ID: {silver_run_id}")

def get_last_processed_ts(table_name):
    if spark.catalog.tableExists(control_table):
        try:
            return spark.table(control_table).filter(col("table_name") == table_name).select(spark_max("last_processed_ts")).collect()[0][0]
        except: return None
    return None

def upsert_to_silver(df, target_table, pk):
    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark, target_table)
        dt.alias("t").merge(df.alias("s"), f"t.{pk} = s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").saveAsTable(target_table)

def process_table(table_name, pk, ts_col, transform_logic, validation_logic):
    bronze_table = f"work.bronze_schema.{table_name}_raw"
    target_table = f"work.silver_schema.{table_name}_transformed"
    quarantine_table = f"work.silver_schema.{table_name}_quarantine"
    
    last_ts = get_last_processed_ts(table_name)
    df_raw = spark.read.table(bronze_table)
    df_inc = df_raw.filter(col("bronze_ingested_at") > lit(last_ts)) if last_ts else df_raw
    
    count = df_inc.count()
    print(f"[{table_name}] Found {count} new records.")
    
    if count == 0: return

    # Deduplication
    window = Window.partitionBy(pk).orderBy(col(ts_col).cast("timestamp").desc(), col("bronze_ingested_at").desc())
    df_dedup = df_inc.withColumn("row_rank", row_number().over(window)).filter(col("row_rank") == 1).drop("row_rank")
    
    # Transformation
    df_transformed = transform_logic(df_dedup).withColumn("silver_run_id", lit(silver_run_id))
    
    # Validation & Quarantine
    df_validated = validation_logic(df_transformed)
    df_good = df_validated.filter(col("is_valid") == True).drop("is_valid", "quarantine_reason")
    df_bad = df_validated.filter(col("is_valid") == False).drop("is_valid").withColumn("quarantine_ts", current_timestamp())
    
    print(f"[{table_name}] Valid: {df_good.count()} | Quarantined: {df_bad.count()}")
    
    # Upsert Good & Append Bad
    upsert_to_silver(df_good, target_table, pk)
    if df_bad.count() > 0:
        df_bad.write.format("delta").mode("append").saveAsTable(quarantine_table)
        
    # Update State
    max_ts = df_inc.select(spark_max("bronze_ingested_at")).collect()[0][0]
    ctrl_df = spark.createDataFrame([(table_name, max_ts, datetime.now())], ["table_name", "last_processed_ts", "updated_at"])
    dt = DeltaTable.forName(spark, control_table)
    dt.alias("t").merge(ctrl_df.alias("s"), "t.table_name = s.table_name").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# ==========================================
# (Transform & Validate Definitions)
# ==========================================

def clean_money(c):
    return regexp_replace(regexp_replace(regexp_replace(col(c), r"[\$,\s]", ""), ",", "."), r"\s+", "").cast("double")

def t_orders(df):
    return (df
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("order_amount", clean_money("order_amount"))
        .withColumn("created_at", to_timestamp("created_at"))
        .withColumn("order_date", to_date("created_at"))
        .withColumn("order_year", year("created_at"))
        .withColumn("order_month", month("created_at"))
        .withColumn("order_day", dayofmonth("created_at"))
    )

def v_orders(df):
    return df.withColumn("is_valid", col("customer_id").isNotNull() & col("product_id").isNotNull() & (col("order_amount") > 0) & (col("order_status") != "")).withColumn("quarantine_reason", when(~col("is_valid"), "Invalid order fields").otherwise("Valid"))

def t_products(df):
    return (df
        .withColumn("product_name", upper(trim(regexp_replace(col("product_name"), r"[-_]", " "))))
        .withColumn("category", when(upper(trim(col("category"))).contains("ELECTRNICS"), "ELECTRONICS").otherwise(upper(trim(col("category")))))
        .withColumn("price", clean_money("price"))
    ).drop("price_raw")

def v_products(df):
    return df.withColumn("is_valid", col("product_name").isNotNull() & col("category").isNotNull() & (col("price") > 0)).withColumn("quarantine_reason", when(~col("is_valid"), "Invalid product fields").otherwise("Valid"))

def t_payments(df):
    return (df
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("paid_amount", clean_money("paid_amount"))
        .withColumn("processed_at", to_timestamp("processed_at"))
    )

def v_payments(df):
    return df.withColumn("is_valid", col("order_id").isNotNull() & col("payment_status").isNotNull() & (col("paid_amount") > 0)).withColumn("quarantine_reason", when(~col("is_valid"), "Invalid payment fields").otherwise("Valid"))

# ==========================================
# (Execution)
# ==========================================
process_table("orders", "order_id", "updated_at", t_orders, v_orders)
process_table("products", "product_id", "updated_at", t_products, v_products)
process_table("payments", "payment_id", "processed_at", t_payments, v_payments)

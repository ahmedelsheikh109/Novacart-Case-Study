from pyspark.sql.functions import col, lit, max as spark_max, when, greatest, countDistinct, sum as spark_sum, avg, count
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

# ==========================================
# (Configurations)
# ==========================================
spark.sql("USE CATALOG work")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold_schema")

control_table = "work.gold_schema.processing_control"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {control_table} (
        table_name STRING,
        last_processed_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

gold_run_id = str(uuid.uuid4())
run_ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
run_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
print(f"Current Gold Run ID: {gold_run_id}")

def get_last_processed_ts(table_name):
    if spark.catalog.tableExists(control_table):
        try:
            return spark.table(control_table).filter(col("table_name") == table_name).select(spark_max("last_processed_ts")).collect()[0][0]
        except: return None
    return None

def upsert_to_gold(df_source, target_table, pk):
    if spark.catalog.tableExists(target_table):
        DeltaTable.forName(spark, target_table).alias("t").merge(
            df_source.alias("s"), f"t.{pk} = s.{pk}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_source.write.format("delta").saveAsTable(target_table)

# ==========================================
# (Incremental Fetch & Processing)
# ==========================================
last_gold_ts = get_last_processed_ts("orders_information")
print(f"Last Processed Silver Timestamp for Gold = {last_gold_ts}")

silver_orders = spark.read.table("work.silver_schema.orders_transformed")
silver_products = spark.read.table("work.silver_schema.products_transformed")
silver_payments = spark.read.table("work.silver_schema.payments_transformed")

changed_orders = silver_orders.filter(col("updated_at") > lit(last_gold_ts)) if last_gold_ts else silver_orders
changed_products = silver_products.filter(col("updated_at") > lit(last_gold_ts)) if last_gold_ts else silver_products
changed_payments = silver_payments.filter(col("updated_at") > lit(last_gold_ts)) if last_gold_ts else silver_payments

count_orders, count_products, count_payments = changed_orders.count(), changed_products.count(), changed_payments.count()
print(f"Changed: Orders={count_orders} | Products={count_products} | Payments={count_payments}")

if count_orders + count_products + count_payments > 0:
    # 1. Identify Impacted Orders
    impacted_orders = changed_orders.select("order_id") \
        .union(changed_payments.select("order_id")) \
        .union(changed_products.join(silver_orders, "product_id").select("order_id")) \
        .distinct()
    
    # 2. Build Gold Fact Table
    gold_delta = (
        silver_orders.alias("o").join(impacted_orders.alias("i"), "order_id")
        .join(silver_products.alias("p"), "product_id")
        .join(silver_payments.alias("py"), "order_id")
        .select(
            col("o.order_id"), col("o.customer_id"), col("p.product_id"), col("p.product_name"), col("p.category"),
            col("p.price").alias("products_price"), col("o.order_status"), col("o.order_amount"), col("py.payment_id"),
            col("py.payment_status"), col("py.paid_amount"), col("o.order_date"), col("o.order_month"), col("o.order_year"),
            greatest(col("o.updated_at"), col("p.updated_at"), col("py.processed_at")).cast("timestamp").alias("gold_update_ts")
        )
        .dropDuplicates(["order_id"])
        .withColumn("payment_completion_ratio", when(col("order_amount") > 0, col("paid_amount") / col("order_amount")).otherwise(lit(0.0)))
        .withColumn("payment_state",
            when(col("order_amount") == 0, "Invalid_order_amount")
            .when(col("payment_completion_ratio") == 0, "Unpaid")
            .when(col("payment_completion_ratio") == 1, "Paid")
            .when(col("payment_completion_ratio") < 1, "Partially_paid")
            .otherwise("Overpaid")
        )
        .withColumn("gold_run_id", lit(gold_run_id))
    )

    upsert_to_gold(gold_delta, "work.gold_schema.orders_information", "order_id")
    
    # 3. Maintain SCD Type 2 History
    if not spark.catalog.tableExists("work.gold_schema.orders_information_scd2"):
        spark.sql("""
            CREATE TABLE work.gold_schema.orders_information_scd2 USING DELTA AS 
            SELECT *, CAST(NULL AS TIMESTAMP) AS valid_from_ts, CAST(NULL AS TIMESTAMP) AS valid_to_ts, TRUE AS is_current 
            FROM work.gold_schema.orders_information WHERE 1=0
        """)
        
    gold_delta.createOrReplaceTempView("gold_delta_view")
    
    spark.sql("""
        MERGE INTO work.gold_schema.orders_information_scd2 t USING gold_delta_view s ON t.order_id = s.order_id AND t.is_current = true
        WHEN MATCHED AND (NOT(t.order_status <=> s.order_status) OR NOT(t.order_amount <=> s.order_amount) OR NOT(t.paid_amount <=> s.paid_amount) OR NOT(t.payment_id <=> s.payment_id) OR NOT(t.category <=> s.category) OR NOT(t.product_name <=> s.product_name) OR NOT(t.products_price <=> s.products_price)) 
        THEN UPDATE SET t.valid_to_ts = s.gold_update_ts, t.is_current = false
    """)
    spark.sql("""
        INSERT INTO work.gold_schema.orders_information_scd2
        SELECT s.*, s.gold_update_ts as valid_from_ts, CAST(NULL AS TIMESTAMP) as valid_to_ts, TRUE as is_current
        FROM gold_delta_view s LEFT JOIN work.gold_schema.orders_information_scd2 t ON s.order_id = t.order_id AND t.is_current = true
        WHERE t.order_id IS NULL OR (NOT(t.order_status <=> s.order_status) OR NOT(t.order_amount <=> s.order_amount) OR NOT(t.paid_amount <=> s.paid_amount) OR NOT(t.payment_id <=> s.payment_id) OR NOT(t.category <=> s.category) OR NOT(t.product_name <=> s.product_name) OR NOT(t.products_price <=> s.products_price))
    """)

    # 4. Aggregations (Category Performance)
    impacted_categories = gold_delta.select("category").filter(col("category").isNotNull()).distinct()
    category_perf_delta = (
        spark.read.table("work.gold_schema.orders_information").join(impacted_categories, "category")
        .groupBy("category").agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum(when(col("order_amount") > 0, col("order_amount")).otherwise(lit(0.0))).alias("Gross_Merchandise_Value"),
            spark_sum(when(col("paid_amount") > 0, col("paid_amount")).otherwise(lit(0.0))).alias("Total_Paid_Amount"),
            avg(col("payment_completion_ratio")).alias("Average_Payment_Completion_Ratio"),
            (spark_sum(when(col("payment_status") == "FAILED", 1).otherwise(0)) / count("*")).alias("Payment_Failure_Rate")
        )
    )
    upsert_to_gold(category_perf_delta, "work.gold_schema.category_performance", "category")

    # 5. Snapshots to Volumes
    spark.sql("CREATE VOLUME IF NOT EXISTS work.gold_schema.gold_snapshots_vol")
    for tbl in ["orders_information", "category_performance"]:
        spark.table(f"work.gold_schema.{tbl}").write.mode("overwrite").format("parquet").save(f"/Volumes/work/gold_schema/gold_snapshots_vol/gold_latest/{tbl}")
        spark.table(f"work.gold_schema.{tbl}").write.mode("overwrite").format("parquet").save(f"/Volumes/work/gold_schema/gold_snapshots_vol/gold_snapshots/{tbl}/run_date={run_date_str}/run_ts={run_ts_str}")

    # 6. Update Control State
    max_ts = gold_delta.select(spark_max("gold_update_ts")).collect()[0][0]
    ctrl_df = spark.createDataFrame([("orders_information", max_ts, datetime.now())], ["table_name", "last_processed_ts", "updated_at"])
    DeltaTable.forName(spark, control_table).alias("t").merge(ctrl_df.alias("s"), "t.table_name = s.table_name").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

else:
    print("No new rows to process in Gold layer.")

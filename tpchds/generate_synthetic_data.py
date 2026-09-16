# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DS-like synthetic data generator
# MAGIC
# MAGIC Generates all 24 tables from `tpchds/schema.sql` as managed Delta tables.
# MAGIC The default configuration creates **100 million sales rows** (40M store,
# MAGIC 30M catalog, and 30M web), plus returns, inventory, and dimensions.
# MAGIC Generation is deterministic and fully distributed; it does not collect data
# MAGIC or use Python UDFs.

# COMMAND ----------

from pyspark.sql import functions as F

# Configuration constants. Edit these values before running the notebook.
CATALOG = ""                 # Blank uses the current catalog.
SCHEMA = "tpchds"            # Target schema.
SALES_ROWS = 100_000_000      # Total across store, catalog, and web sales.
INVENTORY_ROWS = 5_000_000
SEED = 42
OUTPUT_PARTITIONS = 400
WRITE_MODE = "overwrite"      # Supported: "overwrite" or "errorifexists".

assert SALES_ROWS >= 3, "sales_rows must be at least 3"
assert INVENTORY_ROWS >= 0
assert OUTPUT_PARTITIONS > 0
assert WRITE_MODE in {"overwrite", "errorifexists"}

NAMESPACE = f"`{CATALOG}`.`{SCHEMA}`" if CATALOG else f"`{SCHEMA}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NAMESPACE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model and scale

# COMMAND ----------

# Compact Spark schemas. STRING replaces fixed-width character types while
# preserving every column in the companion Delta DDL.
SCHEMAS = {
    "income_band": "ib_income_band_sk:bigint,ib_lower_bound:int,ib_upper_bound:int",
    "reason": "r_reason_sk:bigint,r_reason_id:string,r_reason_desc:string",
    "ship_mode": "sm_ship_mode_sk:bigint,sm_ship_mode_id:string,sm_type:string,sm_code:string,sm_carrier:string,sm_contract:string",
    "warehouse": "w_warehouse_sk:bigint,w_warehouse_id:string,w_warehouse_name:string,w_warehouse_sq_ft:int,w_street_number:string,w_street_name:string,w_street_type:string,w_suite_number:string,w_city:string,w_county:string,w_state:string,w_zip:string,w_country:string,w_gmt_offset:decimal(5;2)",
    "customer_address": "ca_address_sk:bigint,ca_address_id:string,ca_street_number:string,ca_street_name:string,ca_street_type:string,ca_suite_number:string,ca_city:string,ca_county:string,ca_state:string,ca_zip:string,ca_country:string,ca_gmt_offset:decimal(5;2),ca_location_type:string",
    "customer_demographics": "cd_demo_sk:bigint,cd_gender:string,cd_marital_status:string,cd_education_status:string,cd_purchase_estimate:int,cd_credit_rating:string,cd_dep_count:smallint,cd_dep_employed_count:smallint,cd_dep_college_count:smallint",
    "household_demographics": "hd_demo_sk:bigint,hd_income_band_sk:bigint,hd_buy_potential:string,hd_dep_count:smallint,hd_vehicle_count:smallint",
    "customer": "c_customer_sk:bigint,c_customer_id:string,c_current_cdemo_sk:bigint,c_current_hdemo_sk:bigint,c_current_addr_sk:bigint,c_first_shipto_date_sk:bigint,c_first_sales_date_sk:bigint,c_salutation:string,c_first_name:string,c_last_name:string,c_preferred_cust_flag:boolean,c_birth_day:smallint,c_birth_month:smallint,c_birth_year:int,c_birth_country:string,c_login:string,c_email_address:string,c_last_review_date_sk:bigint",
    "item": "i_item_sk:bigint,i_item_id:string,i_rec_start_date:date,i_rec_end_date:date,i_item_desc:string,i_current_price:decimal(15;2),i_wholesale_cost:decimal(15;2),i_brand_id:int,i_brand:string,i_class_id:int,i_class:string,i_category_id:int,i_category:string,i_manufact_id:int,i_manufact:string,i_size:string,i_formulation:string,i_color:string,i_units:string,i_container:string,i_manager_id:int,i_product_name:string",
    "promotion": "p_promo_sk:bigint,p_promo_id:string,p_start_date_sk:bigint,p_end_date_sk:bigint,p_item_sk:bigint,p_cost:decimal(15;2),p_response_target:int,p_promo_name:string,p_channel_dmail:boolean,p_channel_email:boolean,p_channel_catalog:boolean,p_channel_tv:boolean,p_channel_radio:boolean,p_channel_press:boolean,p_channel_event:boolean,p_channel_demo:boolean,p_channel_details:string,p_purpose:string,p_discount_active:boolean",
    "store": "s_store_sk:bigint,s_store_id:string,s_rec_start_date:date,s_rec_end_date:date,s_closed_date_sk:bigint,s_store_name:string,s_number_employees:int,s_floor_space:int,s_hours:string,s_manager:string,s_market_id:int,s_geography_class:string,s_market_desc:string,s_market_manager:string,s_division_id:int,s_division_name:string,s_company_id:int,s_company_name:string,s_street_number:string,s_street_name:string,s_street_type:string,s_suite_number:string,s_city:string,s_county:string,s_state:string,s_zip:string,s_country:string,s_gmt_offset:decimal(5;2),s_tax_percentage:decimal(5;2)",
    "call_center": "cc_call_center_sk:bigint,cc_call_center_id:string,cc_rec_start_date:date,cc_rec_end_date:date,cc_closed_date_sk:bigint,cc_open_date_sk:bigint,cc_name:string,cc_class:string,cc_employees:int,cc_sq_ft:int,cc_hours:string,cc_manager:string,cc_market_id:int,cc_market_class:string,cc_market_desc:string,cc_market_manager:string,cc_division:int,cc_division_name:string,cc_company:int,cc_company_name:string,cc_street_number:string,cc_street_name:string,cc_street_type:string,cc_suite_number:string,cc_city:string,cc_county:string,cc_state:string,cc_zip:string,cc_country:string,cc_gmt_offset:decimal(5;2),cc_tax_percentage:decimal(5;2)",
    "catalog_page": "cp_catalog_page_sk:bigint,cp_catalog_page_id:string,cp_start_date_sk:bigint,cp_end_date_sk:bigint,cp_department:string,cp_catalog_number:int,cp_catalog_page_number:int,cp_description:string,cp_type:string",
    "web_site": "web_site_sk:bigint,web_site_id:string,web_rec_start_date:date,web_rec_end_date:date,web_name:string,web_open_date_sk:bigint,web_close_date_sk:bigint,web_class:string,web_manager:string,web_market_id:int,web_market_class:string,web_market_desc:string,web_market_manager:string,web_company_id:int,web_company_name:string,web_street_number:string,web_street_name:string,web_street_type:string,web_suite_number:string,web_city:string,web_county:string,web_state:string,web_zip:string,web_country:string,web_gmt_offset:decimal(5;2),web_tax_percentage:decimal(5;2)",
    "web_page": "wp_web_page_sk:bigint,wp_web_page_id:string,wp_rec_start_date:date,wp_rec_end_date:date,wp_creation_date_sk:bigint,wp_access_date_sk:bigint,wp_autogen_flag:boolean,wp_customer_sk:bigint,wp_url:string,wp_type:string,wp_char_count:int,wp_link_count:int,wp_image_count:int,wp_max_ad_count:int",
}

FACT_SCHEMAS = {
    "store_sales": "ss_sold_date_sk:bigint,ss_sold_time_sk:bigint,ss_item_sk:bigint,ss_customer_sk:bigint,ss_cdemo_sk:bigint,ss_hdemo_sk:bigint,ss_addr_sk:bigint,ss_store_sk:bigint,ss_promo_sk:bigint,ss_ticket_number:bigint,ss_quantity:int,ss_wholesale_cost:decimal(15;2),ss_list_price:decimal(15;2),ss_sales_price:decimal(15;2),ss_ext_discount_amt:decimal(15;2),ss_ext_sales_price:decimal(15;2),ss_ext_wholesale_cost:decimal(15;2),ss_ext_list_price:decimal(15;2),ss_ext_tax:decimal(15;2),ss_coupon_amt:decimal(15;2),ss_net_paid:decimal(15;2),ss_net_paid_inc_tax:decimal(15;2),ss_net_profit:decimal(15;2)",
    "store_returns": "sr_returned_date_sk:bigint,sr_return_time_sk:bigint,sr_item_sk:bigint,sr_customer_sk:bigint,sr_cdemo_sk:bigint,sr_hdemo_sk:bigint,sr_addr_sk:bigint,sr_store_sk:bigint,sr_reason_sk:bigint,sr_ticket_number:bigint,sr_return_quantity:int,sr_return_amt:decimal(15;2),sr_return_tax:decimal(15;2),sr_return_amt_inc_tax:decimal(15;2),sr_fee:decimal(15;2),sr_return_ship_cost:decimal(15;2),sr_refunded_cash:decimal(15;2),sr_reversed_charge:decimal(15;2),sr_store_credit:decimal(15;2),sr_net_loss:decimal(15;2)",
    "catalog_sales": "cs_sold_date_sk:bigint,cs_sold_time_sk:bigint,cs_ship_date_sk:bigint,cs_bill_customer_sk:bigint,cs_bill_cdemo_sk:bigint,cs_bill_hdemo_sk:bigint,cs_bill_addr_sk:bigint,cs_ship_customer_sk:bigint,cs_ship_cdemo_sk:bigint,cs_ship_hdemo_sk:bigint,cs_ship_addr_sk:bigint,cs_call_center_sk:bigint,cs_catalog_page_sk:bigint,cs_ship_mode_sk:bigint,cs_warehouse_sk:bigint,cs_item_sk:bigint,cs_promo_sk:bigint,cs_order_number:bigint,cs_quantity:int,cs_wholesale_cost:decimal(15;2),cs_list_price:decimal(15;2),cs_sales_price:decimal(15;2),cs_ext_discount_amt:decimal(15;2),cs_ext_sales_price:decimal(15;2),cs_ext_wholesale_cost:decimal(15;2),cs_ext_list_price:decimal(15;2),cs_ext_tax:decimal(15;2),cs_coupon_amt:decimal(15;2),cs_ext_ship_cost:decimal(15;2),cs_net_paid:decimal(15;2),cs_net_paid_inc_tax:decimal(15;2),cs_net_paid_inc_ship:decimal(15;2),cs_net_paid_inc_ship_tax:decimal(15;2),cs_net_profit:decimal(15;2)",
    "catalog_returns": "cr_returned_date_sk:bigint,cr_returned_time_sk:bigint,cr_item_sk:bigint,cr_refunded_customer_sk:bigint,cr_refunded_cdemo_sk:bigint,cr_refunded_hdemo_sk:bigint,cr_refunded_addr_sk:bigint,cr_returning_customer_sk:bigint,cr_returning_cdemo_sk:bigint,cr_returning_hdemo_sk:bigint,cr_returning_addr_sk:bigint,cr_call_center_sk:bigint,cr_catalog_page_sk:bigint,cr_ship_mode_sk:bigint,cr_warehouse_sk:bigint,cr_reason_sk:bigint,cr_order_number:bigint,cr_return_quantity:int,cr_return_amount:decimal(15;2),cr_return_tax:decimal(15;2),cr_return_amt_inc_tax:decimal(15;2),cr_fee:decimal(15;2),cr_return_ship_cost:decimal(15;2),cr_refunded_cash:decimal(15;2),cr_reversed_charge:decimal(15;2),cr_store_credit:decimal(15;2),cr_net_loss:decimal(15;2)",
    "web_sales": "ws_sold_date_sk:bigint,ws_sold_time_sk:bigint,ws_ship_date_sk:bigint,ws_item_sk:bigint,ws_bill_customer_sk:bigint,ws_bill_cdemo_sk:bigint,ws_bill_hdemo_sk:bigint,ws_bill_addr_sk:bigint,ws_ship_customer_sk:bigint,ws_ship_cdemo_sk:bigint,ws_ship_hdemo_sk:bigint,ws_ship_addr_sk:bigint,ws_web_page_sk:bigint,ws_web_site_sk:bigint,ws_ship_mode_sk:bigint,ws_warehouse_sk:bigint,ws_promo_sk:bigint,ws_order_number:bigint,ws_quantity:int,ws_wholesale_cost:decimal(15;2),ws_list_price:decimal(15;2),ws_sales_price:decimal(15;2),ws_ext_discount_amt:decimal(15;2),ws_ext_sales_price:decimal(15;2),ws_ext_wholesale_cost:decimal(15;2),ws_ext_list_price:decimal(15;2),ws_ext_tax:decimal(15;2),ws_coupon_amt:decimal(15;2),ws_ext_ship_cost:decimal(15;2),ws_net_paid:decimal(15;2),ws_net_paid_inc_tax:decimal(15;2),ws_net_paid_inc_ship:decimal(15;2),ws_net_paid_inc_ship_tax:decimal(15;2),ws_net_profit:decimal(15;2)",
    "web_returns": "wr_returned_date_sk:bigint,wr_returned_time_sk:bigint,wr_item_sk:bigint,wr_refunded_customer_sk:bigint,wr_refunded_cdemo_sk:bigint,wr_refunded_hdemo_sk:bigint,wr_refunded_addr_sk:bigint,wr_returning_customer_sk:bigint,wr_returning_cdemo_sk:bigint,wr_returning_hdemo_sk:bigint,wr_returning_addr_sk:bigint,wr_web_page_sk:bigint,wr_reason_sk:bigint,wr_order_number:bigint,wr_return_quantity:int,wr_return_amt:decimal(15;2),wr_return_tax:decimal(15;2),wr_return_amt_inc_tax:decimal(15;2),wr_fee:decimal(15;2),wr_return_ship_cost:decimal(15;2),wr_refunded_cash:decimal(15;2),wr_reversed_charge:decimal(15;2),wr_account_credit:decimal(15;2),wr_net_loss:decimal(15;2)",
    "inventory": "inv_date_sk:bigint,inv_item_sk:bigint,inv_warehouse_sk:bigint,inv_quantity_on_hand:int",
}

SIZES = {
    "date_dim": 7_305, "time_dim": 86_400, "income_band": 20,
    "reason": 50, "ship_mode": 20, "warehouse": 50,
    "customer_address": 1_000_000, "customer_demographics": 200_000,
    "household_demographics": 50_000, "customer": 2_000_000,
    "item": 500_000, "promotion": 10_000, "store": 1_000,
    "call_center": 100, "catalog_page": 20_000, "web_site": 100,
    "web_page": 100_000,
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deterministic expression generator

# COMMAND ----------

FK_SIZE = {
    "date_sk": SIZES["date_dim"], "time_sk": SIZES["time_dim"],
    "item_sk": SIZES["item"], "customer_sk": SIZES["customer"],
    "cdemo_sk": SIZES["customer_demographics"], "hdemo_sk": SIZES["household_demographics"],
    "addr_sk": SIZES["customer_address"], "store_sk": SIZES["store"],
    "promo_sk": SIZES["promotion"], "reason_sk": SIZES["reason"],
    "ship_mode_sk": SIZES["ship_mode"], "warehouse_sk": SIZES["warehouse"],
    "call_center_sk": SIZES["call_center"], "catalog_page_sk": SIZES["catalog_page"],
    "web_page_sk": SIZES["web_page"], "web_site_sk": SIZES["web_site"],
    "income_band_sk": SIZES["income_band"],
}

def parse_schema(spec):
    return [(part.split(":", 1)[0], part.split(":", 1)[1].replace(";", ",")) for part in spec.split(",")]

def hash_value(column_name):
    return F.xxhash64(F.col("id"), F.lit(column_name), F.lit(SEED))

def fk_cardinality(name):
    for suffix, size in FK_SIZE.items():
        if name.endswith(suffix):
            return size
    return None

def synthetic_column(name, data_type, table_name, primary_key_name=None):
    h = hash_value(name)
    fk_size = fk_cardinality(name)
    if name == primary_key_name:
        return (F.col("id") + 1).cast("bigint").alias(name)
    if name.endswith("_sk") and fk_size is None:
        return (F.col("id") + 1).cast("bigint").alias(name)
    if fk_size:
        return (F.pmod(h, F.lit(fk_size)) + 1).cast(data_type).alias(name)
    if name.endswith("_number") and data_type == "bigint":
        return (F.floor(F.col("id") / 4) + 1).cast("bigint").alias(name)
    if name.endswith("_id"):
        return F.concat(F.lit(table_name[:3].upper() + "-"), F.lpad((F.col("id") + 1).cast("string"), 12, "0")).alias(name)
    if data_type == "boolean":
        return (F.pmod(h, F.lit(2)) == 0).alias(name)
    if data_type == "date":
        offset = F.pmod(h, F.lit(6_940)).cast("int")
        return F.date_add(F.lit("2000-01-01").cast("date"), offset).alias(name)
    if data_type.startswith("decimal"):
        return (F.pmod(h, F.lit(100_000)).cast("double") / 100.0).cast(data_type).alias(name)
    if data_type in ("int", "smallint", "bigint"):
        upper = 20 if data_type == "smallint" else 10_000
        return F.pmod(h, F.lit(upper)).cast(data_type).alias(name)
    if name.endswith("_email_address"):
        return F.concat(F.lit("customer"), (F.col("id") + 1), F.lit("@example.test")).alias(name)
    return F.concat(F.lit(name + "_"), F.pmod(h, F.lit(10_000)).cast("string")).alias(name)

def table_df(table_name, row_count, schema_spec):
    parsed = parse_schema(schema_spec)
    # Every generic dimension's first column is its surrogate primary key.
    # Fact tables instead begin with foreign keys and use their natural
    # composite grain, so they deliberately have no generated primary key.
    primary_key_name = parsed[0][0] if table_name in SCHEMAS else None
    columns = [synthetic_column(name, data_type, table_name, primary_key_name) for name, data_type in parsed]
    return spark.range(row_count, numPartitions=min(OUTPUT_PARTITIONS, max(1, row_count // 100_000 + 1))).select(*columns)

def write_table(table_name, df, partition_columns=None):
    writer = df.repartition(OUTPUT_PARTITIONS).write.format("delta").mode(WRITE_MODE)
    if WRITE_MODE == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.saveAsTable(f"{NAMESPACE}.`{table_name}`")
    print(f"Wrote {table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Date and time dimensions

# COMMAND ----------

dates = (
    spark.range(SIZES["date_dim"])
    .withColumn("d_date", F.date_add(F.lit("2000-01-01").cast("date"), F.col("id").cast("int")))
    .select(
        (F.col("id") + 1).cast("bigint").alias("d_date_sk"),
        F.date_format("d_date", "yyyyMMdd").alias("d_date_id"), "d_date",
        (F.year("d_date") * 12 + F.month("d_date")).alias("d_month_seq"),
        F.weekofyear("d_date").alias("d_week_seq"),
        (F.year("d_date") * 4 + F.quarter("d_date")).alias("d_quarter_seq"),
        F.year("d_date").alias("d_year"), (F.dayofweek("d_date") - 1).cast("smallint").alias("d_dow"),
        F.month("d_date").cast("smallint").alias("d_moy"), F.dayofmonth("d_date").cast("smallint").alias("d_dom"),
        F.quarter("d_date").cast("smallint").alias("d_qoy"), F.year("d_date").alias("d_fy_year"),
        (F.year("d_date") * 4 + F.quarter("d_date")).alias("d_fy_quarter_seq"), F.weekofyear("d_date").alias("d_fy_week_seq"),
        F.date_format("d_date", "EEEE").alias("d_day_name"), F.concat(F.lit("Q"), F.quarter("d_date")).alias("d_quarter_name"),
        F.lit(False).alias("d_holiday"), F.dayofweek("d_date").isin(1, 7).alias("d_weekend"),
        F.lit(False).alias("d_following_holiday"), F.dayofmonth(F.trunc("d_date", "month")).alias("d_first_dom"),
        F.dayofmonth(F.last_day("d_date")).alias("d_last_dom"), (F.col("id") - 364).cast("int").alias("d_same_day_ly"),
        (F.col("id") - 90).cast("int").alias("d_same_day_lq"), F.lit(False).alias("d_current_day"),
        F.lit(False).alias("d_current_week"), F.lit(False).alias("d_current_month"),
        F.lit(False).alias("d_current_quarter"), F.lit(False).alias("d_current_year")
    )
)
write_table("date_dim", dates)

times = spark.range(SIZES["time_dim"]).select(
    (F.col("id") + 1).cast("bigint").alias("t_time_sk"), F.lpad(F.col("id").cast("string"), 6, "0").alias("t_time_id"),
    F.col("id").cast("int").alias("t_time"), F.floor(F.col("id") / 3600).cast("smallint").alias("t_hour"),
    F.floor((F.col("id") % 3600) / 60).cast("smallint").alias("t_minute"), (F.col("id") % 60).cast("smallint").alias("t_second"),
    F.when(F.col("id") < 43200, "AM").otherwise("PM").alias("t_am_pm"),
    F.when(F.col("id") < 28800, "night").when(F.col("id") < 57600, "day").otherwise("evening").alias("t_shift"),
    F.lit("standard").alias("t_sub_shift"),
    F.when((F.col("id") / 3600).between(6, 10), "breakfast").when((F.col("id") / 3600).between(11, 14), "lunch").otherwise("other").alias("t_meal_time")
)
write_table("time_dim", times)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Remaining dimensions

# COMMAND ----------

for table_name, schema_spec in SCHEMAS.items():
    write_table(table_name, table_df(table_name, SIZES[table_name], schema_spec))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Facts, returns, and inventory

# COMMAND ----------

store_rows = SALES_ROWS * 40 // 100
catalog_rows = SALES_ROWS * 30 // 100
web_rows = SALES_ROWS - store_rows - catalog_rows

fact_sizes = {
    "store_sales": store_rows,
    "catalog_sales": catalog_rows,
    "web_sales": web_rows,
    "store_returns": max(1, store_rows * 8 // 100),
    "catalog_returns": max(1, catalog_rows * 10 // 100),
    "web_returns": max(1, web_rows * 8 // 100),
    "inventory": INVENTORY_ROWS,
}

for table_name, row_count in fact_sizes.items():
    df = table_df(table_name, row_count, FACT_SCHEMAS[table_name])
    write_table(table_name, df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation summary

# COMMAND ----------

expected = {**SIZES, **fact_sizes}
summary_rows = []
for table_name, expected_rows in expected.items():
    actual_rows = spark.table(f"{NAMESPACE}.`{table_name}`").count()
    summary_rows.append((table_name, expected_rows, actual_rows, expected_rows == actual_rows))

display(spark.createDataFrame(summary_rows, "table_name string, expected_rows long, actual_rows long, matches boolean").orderBy("table_name"))
assert len(summary_rows) == 24 and all(row[3] for row in summary_rows), "Row-count validation failed"

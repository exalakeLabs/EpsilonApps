# Databricks notebook source
# MAGIC %md
# MAGIC # Tax filing synthetic data generator
# MAGIC Populates the tables created by `ddl.sql` (exalabs.tax). Defaults: 750K employees x 5 tax
# MAGIC years -> ~56M federal and ~90M state detail rows. Set EMPLOYEES=50_000 for a smoke test.
# MAGIC Run `ddl.sql` first. Re-running replaces all tables' contents (INSERT OVERWRITE).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "exalabs", "tax"
EMPLOYEES = 750_000
TAX_YEARS = [2021, 2022, 2023, 2024, 2025]
ENTITIES = 200
SECOND_STATE_PCT = 0.30          # share of employees filing in a 2nd (work) state
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# COMMAND ----------
# MAGIC %md ## Dimensions

# COMMAND ----------

states = ("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
          "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR GU VI AS").split()
no_tax = {"AK", "FL", "NV", "NH", "SD", "TN", "TX", "WA", "WY", "GU", "VI", "AS"}
terr = {"PR", "GU", "VI", "AS"}
spark.createDataFrame(
    [(i + 1, c, f"State {c}", c in terr, c not in no_tax) for i, c in enumerate(states)],
    "state_key int, state_code string, state_name string, is_territory boolean, has_income_tax boolean",
).write.mode("overwrite").insertInto(T("dim_state"), overwrite=True)
assert len(states) == 55

spark.createDataFrame(
    [(y, __import__("datetime").date(y + 1, 4, 15), y == max(TAX_YEARS)) for y in range(2016, 2026)],
    "tax_year int, filing_deadline date, is_current_year boolean",
).write.mode("overwrite").insertInto(T("dim_tax_year"), overwrite=True)

regions = ["Northeast", "South", "Midwest", "West", "International"]
spark.range(1, ENTITIES + 1).select(
    F.col("id").cast("int").alias("entity_key"),
    F.format_string("E%03d", F.col("id")).alias("entity_code"),
    F.format_string("Business Unit %03d", F.col("id")).alias("entity_name"),
    F.array(*[F.lit(r) for r in regions])[(F.col("id") % 5).cast("int")].alias("region"),
    F.when(F.col("id") % 4 == 0, "Subsidiary").otherwise("Division").alias("entity_type"),
).write.mode("overwrite").insertInto(T("dim_entity"), overwrite=True)

named_income = ["Wages", "Bonus", "Equity (RSU/ESPP)", "Taxable interest", "Dividends",
                "Severance", "Relocation", "Fringe benefits"]
cats = ["Earned", "Earned", "Equity", "Investment", "Investment", "Other", "Other", "Other"]
inc = [(i + 1, n, cats[i]) for i, n in enumerate(named_income)]
inc += [(i + 1, f"Other income type {i + 1}", "Other") for i in range(len(inc), 40)]
spark.createDataFrame(inc, "income_type_key int, income_type_name string, income_category string") \
    .write.mode("overwrite").insertInto(T("dim_income_type"), overwrite=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tax-line catalogue (300 rows)
# MAGIC Federal core lines 1-14, state core lines 101-116; the rest are defined-but-unused schedule lines
# MAGIC (realistic dimension size, exercises join pruning).

# COMMAND ----------

fed_core = [  # key, code, name, category, income_type_key
    (1, "F_WAGES", "W-2 wages", "INCOME", 1), (2, "F_BONUS", "Bonus", "INCOME", 2),
    (3, "F_EQUITY", "Equity compensation", "INCOME", 3), (4, "F_INT", "Taxable interest", "INCOME", 4),
    (5, "F_DIV", "Dividends", "INCOME", 5), (6, "F_OTHER", "Other income", "INCOME", 6),
    (7, "F_AGI", "Adjusted gross income", "ADJUSTMENT", None),
    (8, "F_STD_DED", "Standard deduction", "DEDUCTION", None),
    (9, "F_TAXABLE", "Taxable income", "TAXABLE", None),
    (10, "F_TAX", "Federal income tax", "TAX", None), (11, "F_CREDITS", "Credits", "CREDIT", None),
    (12, "F_WITHHELD", "Federal withholding", "PAYMENT", None),
    (13, "F_BAL", "Balance due / (refund)", "PAYMENT", None),
    (14, "F_EFF_RATE_BASIS", "Marginal bracket basis", "TAXABLE", None)]
st_core = [
    (101, "S_FED_AGI", "Federal AGI (starting point)", "INCOME"), (102, "S_ADD", "State additions", "ADJUSTMENT"),
    (103, "S_SUB", "State subtractions", "ADJUSTMENT"), (104, "S_AGI", "State AGI", "ADJUSTMENT"),
    (105, "S_ALLOC_PCT", "Allocation % to this state", "ADJUSTMENT"),
    (106, "S_APPORTIONED", "Apportioned income", "INCOME"), (107, "S_STD_DED", "State standard deduction", "DEDUCTION"),
    (108, "S_TAXABLE", "State taxable income", "TAXABLE"), (109, "S_TAX", "State income tax", "TAX"),
    (110, "S_LOCAL", "Local / city tax", "TAX"), (111, "S_SDI", "State disability insurance", "TAX"),
    (112, "S_PFL", "Paid family leave", "TAX"), (113, "S_CREDITS", "State credits", "CREDIT"),
    (114, "S_WITHHELD", "State withholding", "PAYMENT"), (115, "S_EST", "Estimated payments", "PAYMENT"),
    (116, "S_BAL", "Balance due / (refund)", "PAYMENT")]
lines = [(k, c, n, "FEDERAL", cat, it, k, True) for k, c, n, cat, it in fed_core]
lines += [(k, f"F_SCHED_{k}", f"Federal schedule line {k}", "FEDERAL", "ADJUSTMENT", None, k, False) for k in range(15, 101)]
lines += [(k, c, n, "STATE", cat, None, k - 100, True) for k, c, n, cat in st_core]
lines += [(k, f"S_SCHED_{k}", f"State schedule line {k}", "STATE", "ADJUSTMENT", None, k - 100, False) for k in range(117, 301)]
spark.createDataFrame(lines, "tax_line_key int, tax_line_code string, tax_line_name string, jurisdiction string, "
                      "line_category string, income_type_key int, line_sequence int, is_core_line boolean") \
    .write.mode("overwrite").insertInto(T("dim_tax_line"), overwrite=True)
assert len(lines) == 300

# COMMAND ----------
# MAGIC %md ## Employees (with a type-2 history row for ~10%)

# COMMAND ----------

h = lambda c, salt: F.abs(F.hash(c, F.lit(salt)))
base = spark.range(1, EMPLOYEES + 1).withColumnRenamed("id", "n")
emp_cur = base.select(
    F.col("n").alias("employee_key"),
    F.format_string("EMP%07d", "n").alias("employee_id"),
    F.format_string("Employee %d", "n").alias("employee_name"),
    (h("n", 1) % ENTITIES + 1).cast("int").alias("entity_key"),
    (h("n", 2) % 55 + 1).cast("int").alias("home_state_key"),
    F.when(h("n", 3) % 10 < 5, "Single").when(h("n", 3) % 10 < 9, "Married filing jointly")
     .otherwise("Head of household").alias("filing_status"),
    F.lit("2020-01-01").cast("date").alias("effective_from"),
    F.lit(None).cast("date").alias("effective_to"), F.lit(True).alias("is_current"))
# history: 10% changed entity; old version keeps a surrogate key above the current range
hist = emp_cur.where(h("employee_key", 9) % 10 == 0).select(
    (F.col("employee_key") + EMPLOYEES).alias("employee_key"), "employee_id", "employee_name",
    ((F.col("entity_key") % ENTITIES) + 1).alias("entity_key"), "home_state_key", "filing_status",
    "effective_from", F.lit("2022-12-31").cast("date").alias("effective_to"), F.lit(False).alias("is_current"))
emp_cur.unionByName(hist).write.mode("overwrite").insertInto(T("dim_employee"), overwrite=True)

# COMMAND ----------
# MAGIC %md ## Return-year spine -> fact_return, federal detail, state detail

# COMMAND ----------

yrs = spark.createDataFrame([(y,) for y in TAX_YEARS], "tax_year int")
spine = (emp_cur.select("employee_key", "entity_key", "home_state_key").crossJoin(yrs)
         .withColumn("return_id", F.concat_ws("-", F.col("tax_year"), F.col("employee_key")))
         .withColumn("salary", (40000 + h("employee_key", 4) % 260000) * F.pow(F.lit(1.04), F.col("tax_year") - 2021))
         .withColumn("r", h("employee_key", 5) + F.col("tax_year")))
load_ts = F.current_timestamp()

# fact_return: status mix; most recent year has more returns still in review
status = (F.when(F.col("r") % 100 < F.when(F.col("tax_year") == max(TAX_YEARS), 55).otherwise(86), "COMPLETE")
           .when(F.col("r") % 100 < F.when(F.col("tax_year") == max(TAX_YEARS), 90).otherwise(94), "IN_REVIEW")
           .otherwise("AMENDED"))
spine.select("return_id", "employee_key", "entity_key", "tax_year", status.alias("return_status"),
             F.expr("make_date(tax_year + 1, 1 + int(r % 4), 1 + int(r % 27))").alias("filed_date"),
             load_ts.alias("load_ts")).write.mode("overwrite").insertInto(T("fact_return"), overwrite=True)

# Federal calc. gross split across income types; deterministic amounts so state can reference AGI.
g = (spine
     .withColumn("wages", F.col("salary") * 0.80).withColumn("bonus", F.col("salary") * 0.08)
     .withColumn("equity", F.col("salary") * 0.06 * (h("employee_key", 6) % 3)).withColumn("interest", F.col("salary") * 0.01)
     .withColumn("div", F.col("salary") * 0.01).withColumn("other", F.col("salary") * 0.02)
     .withColumn("gross", F.expr("wages+bonus+equity+interest+div+other"))
     .withColumn("agi", F.col("gross") * 0.95)
     .withColumn("std", F.lit(13850.0) * F.pow(F.lit(1.03), F.col("tax_year") - 2022))
     .withColumn("taxable", F.greatest(F.col("agi") - F.col("std"), F.lit(0.0)))
     .withColumn("tax", F.col("taxable") * (0.10 + 0.27 * F.least(F.col("taxable") / 500000, F.lit(1.0))))
     .withColumn("credits", F.when(h("employee_key", 7) % 5 == 0, F.lit(2000.0)).otherwise(0.0))
     .withColumn("withheld", F.col("tax") * 1.03)
     .withColumn("bal", F.col("tax") - F.col("credits") - F.col("withheld"))
     .withColumn("basis", F.col("taxable")))

stack = ("stack(14, 1,1,wages, 2,2,bonus, 3,3,equity, 4,4,interest, 5,5,div, 6,6,other, "
         "7,cast(null as int),agi, 8,cast(null as int),std, 9,cast(null as int),taxable, 10,cast(null as int),tax, "
         "11,cast(null as int),credits, 12,cast(null as int),withheld, 13,cast(null as int),bal, 14,cast(null as int),basis)"
         " as (tax_line_key, income_type_key, amount)")
fed = g.selectExpr("return_id", "employee_key", "entity_key", "tax_year", stack)
fed = (fed.select("return_id", "employee_key", "entity_key", "tax_year", "tax_line_key", "income_type_key",
                  F.col("amount").cast("decimal(18,2)").alias("amount"), load_ts.alias("load_ts"))
       .repartition(400, "tax_year"))
fed.write.mode("overwrite").insertInto(T("fact_federal_tax_detail"), overwrite=True)

# State: home state gets (1 - alloc), optional work state gets alloc. Starts from federal AGI.
second = (h("employee_key", 8) % 100 < SECOND_STATE_PCT * 100)
alloc = F.when(second, F.lit(0.30)).otherwise(F.lit(0.0))
st_rows = (g.select("return_id", "employee_key", "entity_key", "tax_year", "agi", "home_state_key",
                    "salary", alloc.alias("alloc"),
                    ((h("employee_key", 10) % 55 + 1).cast("int")).alias("work_state_key")))
home = st_rows.withColumn("state_key", F.col("home_state_key")).withColumn("share", 1 - F.col("alloc"))
work = (st_rows.where(F.col("alloc") > 0).withColumn("state_key", F.col("work_state_key"))
        .withColumn("share", F.col("alloc")).where(F.col("state_key") != F.col("home_state_key")))
sx = home.unionByName(work)
rate = (F.lit(0.01) + (F.col("state_key") % 11) * 0.007)
sx = (sx.join(spark.table(T("dim_state")).select("state_key", "has_income_tax"), "state_key")
      .withColumn("add", F.col("agi") * 0.01).withColumn("sub", F.col("agi") * 0.02)
      .withColumn("s_agi", F.col("agi") + F.col("add") - F.col("sub"))
      .withColumn("apport", F.col("s_agi") * F.col("share"))
      .withColumn("sstd", F.lit(5000.0) * F.col("share"))
      .withColumn("staxable", F.greatest(F.col("apport") - F.col("sstd"), F.lit(0.0)))
      .withColumn("stax", F.when(F.col("has_income_tax"), F.col("staxable") * rate).otherwise(0.0))
      .withColumn("local", F.col("stax") * 0.05).withColumn("sdi", F.col("apport") * 0.001)
      .withColumn("pfl", F.col("apport") * 0.0008).withColumn("scred", F.lit(0.0))
      .withColumn("swh", F.col("stax") * 1.02).withColumn("est", F.lit(0.0))
      .withColumn("sbal", F.col("stax") - F.col("scred") - F.col("swh") - F.col("est")))
sstack = ("stack(16, 101,agi, 102,add, 103,sub, 104,s_agi, 105,share*100, 106,apport, 107,sstd, 108,staxable, "
          "109,stax, 110,local, 111,sdi, 112,pfl, 113,scred, 114,swh, 115,est, 116,sbal) as (tax_line_key, amount)")
state = (sx.selectExpr("return_id", "employee_key", "entity_key", "state_key", "tax_year", "agi", sstack)
         .select("return_id", "employee_key", "entity_key", "state_key", "tax_year", "tax_line_key",
                 F.col("amount").cast("decimal(18,2)").alias("amount"),
                 F.col("agi").cast("decimal(18,2)").alias("federal_agi_ref"), load_ts.alias("load_ts"))
         .repartition(400, "tax_year"))
state.write.mode("overwrite").insertInto(T("fact_state_tax_detail"), overwrite=True)

# COMMAND ----------
# MAGIC %md ## Optimize + stats (liquid clustering, column stats for join pruning / CBO)

# COMMAND ----------

for t in ["fact_federal_tax_detail", "fact_state_tax_detail", "fact_return", "dim_employee"]:
    spark.sql(f"OPTIMIZE {T(t)}")
for t in ["fact_federal_tax_detail", "fact_state_tax_detail", "fact_return", "dim_employee", "dim_entity",
          "dim_state", "dim_tax_year", "dim_income_type", "dim_tax_line"]:
    spark.sql(f"ANALYZE TABLE {T(t)} COMPUTE STATISTICS FOR ALL COLUMNS")
for t in ["fact_return", "fact_federal_tax_detail", "fact_state_tax_detail", "dim_employee"]:
    print(t, spark.table(T(t)).count())

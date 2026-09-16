# TPC-DS-like Delta schema and data generator

This directory provides a Databricks Delta analytical model inspired by
TPC-DS. It contains 24 dimension, inventory, sales, and return tables plus a
distributed synthetic-data generator sized for large-scale query testing.

The default generation run produces 100 million sales rows, approximately
8.6 million return rows, 5 million inventory rows, and several million
dimension rows. This is an original test model, not an official TPC benchmark
kit, and it does not include official TPC data, tools, or benchmark queries.

## Files

| File | Purpose |
| --- | --- |
| `schema.sql` | Creates the `tpchds` schema and 24 managed Delta tables. |
| `generate_synthetic_data.py` | Databricks source notebook that generates and writes the synthetic data. |
| `optimize.sql` | Optionally compacts large Delta tables and Z-Orders common filter columns. |
| `drop.sql` | Deletes the schema and all managed tables in it. |

## Model overview

The schema represents three retail sales channels:

- Store: `store_sales` and `store_returns`
- Catalog: `catalog_sales` and `catalog_returns`
- Web: `web_sales` and `web_returns`

Shared dimensions include customers, customer addresses and demographics,
items, promotions, dates, times, warehouses, shipping modes, and return
reasons. Channel-specific dimensions include stores, call centers, catalog
pages, websites, and web pages. The `inventory` fact records item quantities
by warehouse and date.

The model uses `BIGINT` surrogate keys, `STRING` natural identifiers, and
`DECIMAL(15,2)` monetary values. Delta Lake does not enforce the model's
logical primary keys, foreign keys, or uniqueness rules. The generator keeps
foreign-key values within the corresponding dimension-key ranges.

## Prerequisites

Before staging the project, make sure you have:

- A Databricks workspace with Spark compute capable of writing Delta tables.
- A SQL warehouse for running the SQL files, or permission to execute SQL
  from a Databricks notebook.
- A Unity Catalog catalog and schema location where you have `USE CATALOG`,
  `USE SCHEMA`, `CREATE SCHEMA`, and `CREATE TABLE` permissions.
- Permission to create or attach to an all-purpose or job cluster.
- Enough cloud storage and compute capacity for more than 100 million rows.

For the default scale, use a multi-worker cluster. The ideal worker count and
instance type depend on the cloud, runtime, concurrency, and desired completion
time. Start smaller with `SALES_ROWS = 1_000_000` to verify permissions and
configuration before starting the full run.

## Stage the project in Databricks

### Option 1: Databricks Git folder

1. Push this repository to a Git provider accessible from Databricks.
2. In the Databricks workspace, create a Git folder and clone the repository.
3. Open `tpchds/generate_synthetic_data.py`. Databricks recognizes the
   `# Databricks notebook source` header and cell markers as a source notebook.
4. Confirm that `schema.sql`, `optimize.sql`, and `drop.sql` are also visible
   in the `tpchds` directory.

This option is recommended because changes remain version controlled and the
notebook can be updated by pulling from Git.

### Option 2: Manual workspace import

1. Import `generate_synthetic_data.py` into the Databricks workspace as a
   notebook.
2. Copy the contents of each SQL file into a Databricks SQL editor query, or
   import each file into the workspace.
3. Keep the notebook and SQL queries together in a workspace folder so the
   setup, load, maintenance, and cleanup assets are easy to find.

## End-to-end setup

### 1. Select the target catalog

The SQL files use the current catalog and create a schema named `tpchds`. In a
Databricks SQL editor, select the desired catalog before running the DDL:

```sql
USE CATALOG main;
```

Replace `main` with your catalog. The catalog must already exist, and your user
or service principal must be able to create schemas and tables in it.

### 2. Create the schema and empty tables

Run all of `schema.sql` in the same SQL editor session. It performs:

```sql
CREATE SCHEMA IF NOT EXISTS tpchds;
USE SCHEMA tpchds;
```

It then creates all 24 managed tables with `USING DELTA`. The statements use
`IF NOT EXISTS`, so rerunning the DDL does not delete existing data.

Creating the tables in advance is useful for reviewing the model and verifying
permissions. The generator can also create the schema and write the tables by
itself, so this step is optional when you only need generated data.

### 3. Configure the generator

Open `generate_synthetic_data.py` and edit the constants near the top:

```python
CATALOG = ""                 # Blank uses the cluster's current catalog.
SCHEMA = "tpchds"            # Target schema.
SALES_ROWS = 100_000_000      # Store + catalog + web sales rows.
INVENTORY_ROWS = 5_000_000
SEED = 42
OUTPUT_PARTITIONS = 400
WRITE_MODE = "overwrite"      # "overwrite" or "errorifexists".
```

Configuration details:

- `CATALOG`: Set this explicitly, for example `"main"`, to prevent the result
  from depending on the cluster's default catalog. When blank, the notebook
  uses the current catalog.
- `SCHEMA`: Target schema name. The notebook creates it if it does not exist.
- `SALES_ROWS`: Total sales rows. The notebook allocates 40% to store, 30% to
  catalog, and the remainder to web sales.
- `INVENTORY_ROWS`: Number of inventory fact rows.
- `SEED`: Controls deterministic pseudo-random values. Identical settings and
  seed produce the same generated values.
- `OUTPUT_PARTITIONS`: Number of partitions used when writing each table. A
  larger cluster may benefit from more partitions; excessive partitions can
  create many small files.
- `WRITE_MODE`: `overwrite` replaces table data and schema. `errorifexists`
  stops if a target table already exists.

For an initial smoke test, use:

```python
SALES_ROWS = 1_000_000
INVENTORY_ROWS = 100_000
OUTPUT_PARTITIONS = 32
```

### 4. Attach compute

Attach the notebook to an all-purpose cluster or run it as a Databricks job.
The cluster identity needs permission to use the selected catalog and schema
and to create or overwrite managed tables.

For the 100-million-row configuration:

- Use multiple workers rather than a single-node cluster.
- Ensure autoscaling will not scale down too aggressively during writes.
- Choose workers with adequate memory and local disk for Spark shuffles.
- Confirm that the selected catalog's managed storage is configured.
- Avoid running multiple full generation jobs against the same tables.

### 5. Run the notebook

Run all cells from top to bottom. The notebook:

1. Creates the target schema if needed.
2. Defines the 24 table schemas and their scale parameters.
3. Generates deterministic date and time dimensions.
4. Generates the remaining reference and dimension tables.
5. Generates store, catalog, and web sales and returns.
6. Generates inventory records.
7. Writes each result as a managed Delta table.
8. Counts every table and displays a validation summary.

Generation uses Spark SQL expressions and `spark.range`; it does not build the
data on the driver and does not use Python UDFs. Progress is visible in the
Spark UI and notebook cell output. The final validation cell must show
`matches = true` for all 24 tables.

With the defaults, the main fact-table row counts are:

| Table | Rows |
| --- | ---: |
| `store_sales` | 40,000,000 |
| `catalog_sales` | 30,000,000 |
| `web_sales` | 30,000,000 |
| `store_returns` | 3,200,000 |
| `catalog_returns` | 3,000,000 |
| `web_returns` | 2,400,000 |
| `inventory` | 5,000,000 |

### 6. Verify the load

Use Databricks SQL to confirm that the tables are present:

```sql
USE CATALOG main;
USE SCHEMA tpchds;

SHOW TABLES;

SELECT COUNT(*) AS store_sales_rows FROM store_sales;
SELECT MIN(d_date), MAX(d_date) FROM date_dim;
SELECT COUNT(DISTINCT i_item_sk) AS sold_items FROM store_sales;
```

Replace `main` if a different catalog was used. You can also inspect table
metadata and storage details:

```sql
DESCRIBE DETAIL store_sales;
DESCRIBE EXTENDED item;
```

### 7. Optimize the large tables

After a successful large load, run `optimize.sql` in the same target catalog.
It compacts files in the seven largest fact tables and applies Z-Ordering to
their date and item keys, which supports data skipping for common analytical
filters.

Optimization is optional and consumes additional compute. Skip it for a small
smoke test. Databricks environments using predictive optimization or liquid
clustering may not need the supplied Z-Ordering workflow.

### 8. Run a sample analytical query

```sql
SELECT
  d.d_year,
  i.i_category,
  SUM(ss.ss_ext_sales_price) AS sales_amount,
  SUM(ss.ss_net_profit) AS net_profit
FROM store_sales AS ss
JOIN date_dim AS d
  ON ss.ss_sold_date_sk = d.d_date_sk
JOIN item AS i
  ON ss.ss_item_sk = i.i_item_sk
GROUP BY d.d_year, i.i_category
ORDER BY d.d_year, sales_amount DESC;
```

## Rerunning and changing scale

The default `WRITE_MODE = "overwrite"` replaces each target table. This makes
the notebook repeatable, but existing data in those tables will be lost.
Change to `"errorifexists"` when accidental replacement is a concern.

To change scale, update `SALES_ROWS`, `INVENTORY_ROWS`, and potentially
`OUTPUT_PARTITIONS`, then run the entire notebook again. Do not run only the
fact-generation cells after changing dimension sizes or schemas because this
can leave the model inconsistent.

The generator is deterministic for a given seed and configuration, but Delta
file names and physical file layout can differ between runs.

## Cleanup

`drop.sql` contains:

```sql
DROP SCHEMA IF EXISTS tpchds CASCADE;
```

Run it only when you intend to delete all 24 managed tables and their data.
Confirm the active catalog first:

```sql
SELECT current_catalog(), current_schema();
```

Dropping a managed Delta table removes its managed data. Treat this cleanup as
destructive and make any required backup or clone before running it.

## Troubleshooting

- **Permission denied:** Verify `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`,
  and `CREATE TABLE` permissions for the notebook's execution identity.
- **Tables appear in the wrong catalog:** Set `CATALOG` explicitly in the
  notebook and run `USE CATALOG` before executing the SQL files.
- **Run is too slow:** Start with fewer rows, increase appropriate worker
  capacity, and tune `OUTPUT_PARTITIONS` to the available Spark cores.
- **Many small files:** Reduce `OUTPUT_PARTITIONS` for small runs or execute
  `OPTIMIZE` after the load.
- **Out-of-memory or shuffle failures:** Use workers with more memory or local
  storage, increase the worker count, or reduce the generated row counts.
- **Table already exists:** Use `WRITE_MODE = "overwrite"` to replace it, use
  `errorifexists` to fail safely, or choose a different schema.
- **Validation count fails:** Review the failed Spark stage, confirm available
  storage, and rerun the full notebook rather than only the validation cell.

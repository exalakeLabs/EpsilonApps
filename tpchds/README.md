# TPC-DS-like schema

This directory contains a Databricks Delta analytical schema inspired by
TPC-DS. It is intended for development, demonstrations, and query testing; it
is not an official TPC benchmark kit and does not include TPC data or queries.

## Contents

- `schema.sql` creates the `tpchds` schema and its 24 managed Delta tables.
- `optimize.sql` optionally compacts large tables and Z-Orders common filters.
- `drop.sql` removes the schema and every object it contains.
- `generate_synthetic_data.py` is a Databricks source notebook that populates
  all 24 tables as Delta tables. Its default scale is 100 million sales rows.

## Create the Delta schema

Run `schema.sql` in the Databricks SQL editor or with a Databricks SQL CLI.
If the target is in a non-default Unity Catalog catalog, select that catalog
first with `USE CATALOG catalog_name`.

Monetary values use `DECIMAL(15,2)`, surrogate keys use `BIGINT`, and natural
identifiers use `STRING`. Delta does not enforce the model's primary keys,
foreign keys, or uniqueness rules; the generator maintains valid key ranges.

To remove everything:

Run `drop.sql` in Databricks SQL to remove the schema and its managed tables.

## Generate data in Databricks

Import or open `generate_synthetic_data.py` as a Databricks notebook, attach a
Spark cluster, adjust the configuration constants at the top, and run all
cells. The default
sales mix is 40 million store, 30 million catalog, and 30 million web rows.
Return tables add approximately 8.6 million rows, and inventory adds 5 million.

For the default scale, use a multi-worker cluster and make sure the target
catalog has enough storage. The generator overwrites tables by default and
validates every table's row count after writing.

After a large load, `optimize.sql` can be run to compact fact-table files and
Z-Order them by their most common date and item filter columns. Traditional
secondary indexes are not used by Delta Lake.

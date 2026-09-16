-- Optional Databricks maintenance after a large data load.
-- Delta Lake does not use traditional secondary indexes. OPTIMIZE compacts
-- files, while Z-Ordering improves data skipping for common filter columns.

USE SCHEMA tpchds;

OPTIMIZE inventory ZORDER BY (inv_date_sk, inv_item_sk);
OPTIMIZE store_sales ZORDER BY (ss_sold_date_sk, ss_item_sk);
OPTIMIZE store_returns ZORDER BY (sr_returned_date_sk, sr_item_sk);
OPTIMIZE catalog_sales ZORDER BY (cs_sold_date_sk, cs_item_sk);
OPTIMIZE catalog_returns ZORDER BY (cr_returned_date_sk, cr_item_sk);
OPTIMIZE web_sales ZORDER BY (ws_sold_date_sk, ws_item_sk);
OPTIMIZE web_returns ZORDER BY (wr_returned_date_sk, wr_item_sk);


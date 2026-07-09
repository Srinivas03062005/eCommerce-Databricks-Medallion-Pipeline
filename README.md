# eCommerce Medallion Pipeline — README (Detailed)

Overview
--------
This repository implements an eCommerce medallion data pipeline designed to run on Databricks (Spark + Delta Lake). The pipeline follows Bronze → Silver → Gold stages to ingest raw CSV files, clean and standardize data, and produce analytics-ready tables and a denormalized view for dashboarding.

Quick confirmation
------------------
- `1_medallion_processing_dim/1_dim_bronze.py` ingests dimension sources into the Bronze layer: `brands`, `category`, `products`, `customers`, and `date` (written to `ecommerce.bronze.*` tables). It does NOT ingest `order_items` — fact ingestion is implemented under `1_medallion_processing_fact/`.

Repository layout (important files)
----------------------------------
- `1_setup/setup_catalog.py` — Creates `ecommerce` catalog and `bronze`, `silver`, `gold` schemas.
- `1_medallion_processing_dim/1_dim_bronze.py` — Bronze ingestion for dimensions (brands, category, products, customers, date).
- `1_medallion_processing_dim/2_dim_silver.py` — Silver cleaning and type conversions for dimensions; writes `slv_*` tables.
- `1_medallion_processing_dim/3_dim_gold.py` — Builds gold dimension tables (`gld_dim_products`, `gld_dim_customers`, `gld_dim_date`).
- `1_medallion_processing_fact/1_fact_bronze.py` — Bronze ingestion for `order_items` facts.
- `1_medallion_processing_fact/2_fact_silver.py` — Cleans and converts fact columns; writes `slv_order_items`.
- `1_medallion_processing_fact/3_fact_gold.py` — Computes business metrics (gross, discount, net), FX conversion, and writes `gld_fact_order_items`.
- `1_dashboard_code/denormalise_table_query.txt` — SQL to create `ecommerce.gold.fact_transactions_denorm` (joins facts with date and product dims).
- `1_data_sets/ecomm-raw-data/` — Sample CSV inputs used by the pipeline.

How `1_dim_bronze.py` works (high level)
---------------------------------------
- Uses explicit `StructType` schemas for each source to avoid parsing ambiguity.
- Reads raw CSVs from a mounted path (the scripts reference `/Volumes/ecommerce/source_data/raw/...`).
- Adds metadata columns such as `_source_file`, `file_name`, `ingested_at` / `ingest_timestamp`.
- Writes Delta tables to `ecommerce.bronze.<table>` using `.saveAsTable(...).` with `mergeSchema=true`.

Paths and environment
---------------------
- The scripts assume a Databricks Spark session and mounted storage paths. Before running, update `raw_data_path` variables to your DBFS or mounted path (for example: `dbfs:/mnt/ecommerce/source_data/raw/...` or `s3a://...`).
- The code is notebook-style: `%sql` cells and `display()` are used. Execute as Databricks notebooks or adapt to plain Python Spark jobs.

Execution order (recommended)
-----------------------------
1. Run `1_setup/setup_catalog.py` to create catalog/schemas.
2. Run `1_medallion_processing_dim/1_dim_bronze.py` to populate Bronze dimension tables.
3. Run `1_medallion_processing_fact/1_fact_bronze.py` to ingest fact Bronze tables.
4. Run Silver scripts: `2_dim_silver.py` then `2_fact_silver.py`.
5. Run Gold scripts: `3_dim_gold.py` then `3_fact_gold.py`.
6. Create the denormalized view using `1_dashboard_code/denormalise_table_query.txt`.

Quick validation queries
------------------------
Use these in a Databricks SQL cell or a Spark SQL call:

```
SELECT count(*) FROM ecommerce.bronze.brz_brands;
SELECT count(*) FROM ecommerce.silver.slv_products;
SELECT count(*) FROM ecommerce.gold.gld_dim_products;
SELECT count(*) FROM ecommerce.gold.gld_fact_order_items;
```

Findings and recommendations
----------------------------
- Finding: `1_dim_bronze.py` correctly ingests the dimension CSVs listed above and writes them to Bronze. It does not handle `order_items` (the fact ingest lives under `1_medallion_processing_fact/1_fact_bronze.py`).
- Recommendation: Update the raw path variables to your cluster mount before running. Prefer `dbfs:/` or cloud storage URIs for reproducible runs.
- Recommendation: Consider parameterizing the raw path and catalog name (for example via widget or environment variables) for easier reuse across environments.

If you want
-----------
- I can replace the hard-coded `/Volumes/...` paths with a small config at the top of each script and add a brief `Getting Started` section with exact commands to run these notebooks in Databricks.

---
Generated on 2026-07-09

# eCommerce Databricks Medallion Pipeline

## Project Overview 

This project implements a Databricks-based eCommerce data pipeline using a medallion architecture with Bronze, Silver, and Gold layers. It ingests raw CSV data into Delta tables, cleans and transforms the data, and builds BI-ready dimension and fact tables.

## Repository Structure

- `1_setup/setup_catalog.py`
  - Creates the `ecommerce` catalog and Bronze/Silver/Gold schemas.

- `1_medallion_processing_dim/`
  - `1_dim_bronze.py` — Bronze ingestion for dimension data: brands, category, products.
  - `2_dim_silver.py` — Silver transformations for dimension tables: brands, category, products.
  - `3_dim_gold.py` — Gold model build for product dimension and supporting customer/date dimension logic.

- `1_medallion_processing_fact/`
  - `1_fact_bronze.py` — Bronze ingestion for fact data: order items.
  - `2_fact_silver.py` — Silver transformations for order items, data cleaning, and type conversions.
  - `3_fact_gold.py` — Gold fact model build, revenue calculations, currency conversion and enriched sales metrics.

- `1_dashboard_code/denormalise_table_query.txt`
  - SQL view definition for denormalizing order items with product and date dimensions.

- `1_data_sets/`
  - Contains raw seed CSV files used by the pipeline, including brands, category, customers, date, order items, and products.

## Data Flow

1. **Setup**
   - Create the `ecommerce` catalog and schemas: `ecommerce.bronze`, `ecommerce.silver`, `ecommerce.gold`.

2. **Bronze Layer**
   - Ingest raw CSV data into Delta tables using schemas and metadata columns.
   - Dimension tables: `brz_brands`, `brz_category`, `brz_products`.
   - Fact table: `brz_order_items`.

3. **Silver Layer**
   - Clean and standardize raw records.
   - Apply deduplication, string trimming, normalization, type casting, and anomaly handling.
   - Dimension tables: `slv_brands`, `slv_category`, `slv_products`.
   - Fact table: `slv_order_items`.

4. **Gold Layer**
   - Build BI-ready tables with enriched joins and calculated metrics.
   - Product dimension table: `gld_dim_products`.
   - Order fact enrichment includes gross amount, discount amount, sale amount, date key, coupon flag, and INR currency conversion.
   - A denormalized view: `ecommerce.gold.fact_transactions_denorm`.

## Key Patterns and Transformations

- Uses Delta Lake with `mergeSchema=true` for schema evolution.
- Reads CSV source data with explicit Spark schemas to avoid inconsistent parsing.
- Adds metadata columns such as `_source_file`, `ingest_timestamp`, and `processed_time`.
- Cleans textual anomalies, normalizes codes, and fixes invalid values.
- Converts string-based numeric fields into proper numeric types.
- Converts currency values with a hard-coded Fx rate lookup.
- Aggregates and enriches the fact table with date and product dimension attributes.

## How to Run

1. Open the repository in Databricks.
2. Run `1_setup/setup_catalog.py` to create the catalog and schemas.
3. Run `1_medallion_processing_dim/1_dim_bronze.py` and `1_medallion_processing_fact/1_fact_bronze.py` to load Bronze tables.
4. Run `1_medallion_processing_dim/2_dim_silver.py` and `1_medallion_processing_fact/2_fact_silver.py` to build Silver tables.
5. Run `1_medallion_processing_dim/3_dim_gold.py` and `1_medallion_processing_fact/3_fact_gold.py` to build Gold tables.
6. Use `1_dashboard_code/denormalise_table_query.txt` as a reference for creating a denormalized view.

> Note: The notebook source code uses paths like `/Volumes/ecommerce/source_data/raw/...`. Update these paths to match your cluster's mounted storage location before executing.

## Important Notes

- `3_dim_gold.py` references `slv_customers` and `slv_calendar` tables, but ingestion scripts for these tables are not included in this repository.
- The code is written for Databricks notebooks and includes both Python and SQL cells.
- The Gold view in `denormalise_table_query.txt` joins `gld_fact_order_items`, `gld_dim_date`, and `gld_dim_products`.

## Validation

After running the pipeline, validate the results with queries such as:

```sql
SELECT * FROM ecommerce.gold.gld_dim_products LIMIT 5;
SELECT * FROM ecommerce.gold.gld_dim_customers LIMIT 5;
SELECT * FROM ecommerce.gold.fact_transactions_denorm LIMIT 5;
```

## Summary

This repository presents a complete medallion pipeline for eCommerce data using Databricks and Delta Lake. It demonstrates raw data ingestion, transformation, cleaning, and enrichment across Bronze, Silver, and Gold layers, with a focus on product and order analytics.






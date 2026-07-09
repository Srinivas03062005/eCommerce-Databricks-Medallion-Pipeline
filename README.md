# eCommerce Medallion Data Pipeline

An end-to-end **eCommerce Data Pipeline** built using **Databricks, Apache Spark, and Delta Lake** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw CSV datasets into Delta tables, performs data cleansing and standardization, applies business transformations, computes key business metrics, and produces analytics-ready tables optimized for BI reporting.

---

# Project Overview

This project demonstrates a production-style Data Engineering pipeline using the Medallion Architecture.

## Bronze Layer

- Ingests raw CSV datasets into Delta tables
- Uses explicit `StructType` schemas
- Preserves raw source data
- Captures ingestion metadata such as:
  - Source file
  - Ingestion timestamp

---

## Silver Layer

- Cleans and validates data
- Removes duplicate records
- Handles null values
- Performs type casting
- Standardizes column formats
- Filters invalid records

---

## Gold Layer

- Builds analytics-ready dimensional models
- Computes business KPIs including:
  - Gross Amount
  - Discount Amount
  - Net Amount
- Performs FX (Foreign Exchange) conversions
- Produces denormalized reporting tables for BI dashboards

---

# Architecture

```
                Raw CSV Files
                      │
                      ▼
            ┌─────────────────┐
            │     Bronze       │
            │ Raw Ingestion    │
            └─────────────────┘
                      │
                      ▼
            ┌─────────────────┐
            │     Silver       │
            │ Clean & Validate │
            └─────────────────┘
                      │
                      ▼
            ┌─────────────────┐
            │      Gold        │
            │ Business Models  │
            └─────────────────┘
                      │
                      ▼
          Reporting & BI Dashboards
```

---

# Repository Structure

```
.
├── 1_setup/
│   └── setup_catalog.py
│
├── 1_medallion_processing_dim/
│   ├── 1_dim_bronze.py
│   ├── 2_dim_silver.py
│   └── 3_dim_gold.py
│
├── 1_medallion_processing_fact/
│   ├── 1_fact_bronze.py
│   ├── 2_fact_silver.py
│   └── 3_fact_gold.py
│
├── 1_dashboard_code/
│   └── denormalise_table_query.txt
│
└── 1_data_sets/
    └── ecomm-raw-data/
```

---

# Directory Details

## 1_setup

### setup_catalog.py

Creates the Unity Catalog objects required for the project:

- `ecommerce` Catalog
- `bronze` Schema
- `silver` Schema
- `gold` Schema
- `source_data` Schema

---

# 1_medallion_processing_dim

## 1_dim_bronze.py

Loads the raw dimension datasets into the **Bronze** layer as Delta tables.

### Bronze Tables Created

- `brz_brands`
- `brz_category`
- `brz_products`
- `brz_customers`
- `brz_calendar`

Each table is ingested using explicit schemas and includes ingestion metadata for data lineage and auditing.

---

## 2_dim_silver.py

Processes the Bronze dimension tables into the **Silver** layer by:

- Removing duplicate records
- Cleaning invalid and null values
- Casting columns to appropriate data types
- Standardizing data formats

### Silver Tables Created

- `slv_brands`
- `slv_category`
- `slv_products`
- `slv_customers`
- `slv_calendar`

---

## 3_dim_gold.py

Builds analytics-ready dimension tables in the **Gold** layer.

### Gold Tables Created

- `gld_dim_products`
- `gld_dim_customers`
- `gld_dim_date`

These tables are optimized for joins with fact tables and downstream analytics.

---

# 1_medallion_processing_fact

## 1_fact_bronze.py

Loads the raw **order_items** transactional dataset into the Bronze layer.

### Bronze Table Created

- `brz_order_items`

---

## 2_fact_silver.py

Processes Bronze transaction data into the Silver layer by:

- Removing duplicate transactions
- Cleaning invalid records
- Standardizing data types
- Validating transactional data

### Silver Table Created

- `slv_order_items`

---

## 3_fact_gold.py

Applies business logic to create the final Gold fact table.

Business transformations include:

- Gross Amount calculation
- Discount Amount calculation
- Net Amount calculation
- FX Currency conversion

### Gold Table Created

- `gld_fact_order_items`

---

# 1_dashboard_code

## denormalise_table_query.txt

Contains the SQL query that creates the final reporting table.

### Reporting Table Created

- `fact_transactions_denorm`

The query joins the following Gold tables:

- `gld_fact_order_items`
- `gld_dim_products`
- `gld_dim_customers`
- `gld_dim_date`

to create a denormalized reporting table optimized for business intelligence and analytics.

Compatible with:

- Power BI
- Tableau
- Databricks SQL Dashboards

---

# Environment Configuration

The notebooks are designed to run inside Databricks.

Recommended configuration:

```python
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("raw_data_path", "dbfs:/mnt/ecommerce/source_data/raw/")

ENV = dbutils.widgets.get("environment")
RAW_DATA_PATH = dbutils.widgets.get("raw_data_path")
```

This allows the same notebooks to be used across development, testing, and production environments.

---

# Pipeline Execution Order

Run the notebooks in the following sequence:

```
setup_catalog.py
        │
        ▼
1_dim_bronze.py
        │
        ▼
1_fact_bronze.py
        │
        ▼
2_dim_silver.py
        │
        ▼
2_fact_silver.py
        │
        ▼
3_dim_gold.py
        │
        ▼
3_fact_gold.py
        │
        ▼
denormalise_table_query.txt
```

---

## Step 1

Run:

```
setup_catalog.py
```

Creates the catalog and schemas.

---

## Step 2

Run:

```
1_dim_bronze.py
```

Loads all dimension datasets into the Bronze layer.

---

## Step 3

Run:

```
1_fact_bronze.py
```

Loads transactional data into the Bronze layer.

---

## Step 4

Run:

```
2_dim_silver.py
```

Cleans and standardizes dimension tables.

---

## Step 5

Run:

```
2_fact_silver.py
```

Processes transactional data into the Silver layer.

---

## Step 6

Run:

```
3_dim_gold.py
```

Builds Gold dimension tables.

---

## Step 7

Run:

```
3_fact_gold.py
```

Creates the final Gold fact table with business metrics.

---

## Step 8

Execute:

```
denormalise_table_query.txt
```

Creates the final reporting table:

```
fact_transactions_denorm
```

---

# Data Validation

Verify data at each layer using SQL.

## Bronze

```sql
SELECT COUNT(*)
FROM ecommerce.bronze.brz_brands;
```

---

## Silver

```sql
SELECT COUNT(*)
FROM ecommerce.silver.slv_products;
```

---

## Gold Dimensions

```sql
SELECT COUNT(*)
FROM ecommerce.gold.gld_dim_products;
```

---

## Gold Facts

```sql
SELECT COUNT(*)
FROM ecommerce.gold.gld_fact_order_items;
```

---

## Final Reporting Table

```sql
SELECT COUNT(*)
FROM ecommerce.gold.fact_transactions_denorm;
```

---

# Best Practices Implemented

### Explicit Schema Enforcement

- Uses `StructType` schemas for all raw datasets
- Prevents schema drift
- Improves ingestion reliability

---

### Delta Lake Storage

- All layers are stored as Delta tables
- Supports ACID transactions
- Improves query performance and reliability

---

### Auditability & Data Lineage

Every Bronze table captures:

- Source file
- Ingestion timestamp

making debugging and lineage tracking easier.

---

### Schema Evolution

Tables are written using:

```python
.saveAsTable(...)
```

with:

```python
mergeSchema=True
```

allowing new columns to be added without breaking existing pipelines.

---

### Data Quality

The Silver layer performs:

- Duplicate removal
- Null handling
- Type validation
- Data standardization
- Invalid record filtering

---

### Business Transformations

The Gold layer computes:

- Gross Sales
- Discount Amount
- Net Sales
- FX Currency Conversion
- Analytics-ready Fact Tables

---

# Technologies Used

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- Databricks SQL
- SQL
- Python

---

# Pipeline Outcome

This project demonstrates a production-style Medallion Architecture pipeline that:

- Ingests raw eCommerce datasets into Delta tables
- Cleans and validates data using the Silver layer
- Builds trusted Gold dimension and fact tables
- Computes business metrics and KPIs
- Creates a denormalized reporting table for BI dashboards
- Enables scalable analytics using Databricks, Spark, and Delta Lake




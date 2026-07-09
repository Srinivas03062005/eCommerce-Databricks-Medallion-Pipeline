# eCommerce Medallion Data Pipeline

An end-to-end **eCommerce Data Pipeline** built on **Databricks, Apache Spark, and Delta Lake** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw CSV datasets, performs data cleaning and standardization, applies business transformations, calculates KPIs, and produces analytics-ready tables optimized for BI dashboards and reporting.

---

# Project Overview

This project demonstrates a modern Data Engineering pipeline using the Medallion Architecture.

### Bronze Layer
- Ingests raw CSV datasets
- Uses explicit `StructType` schemas
- Stores raw data without modifications
- Tracks metadata such as:
  - Source file
  - Ingestion timestamp

### Silver Layer
- Cleans and validates data
- Removes duplicates
- Handles null values
- Performs type casting
- Standardizes column formats
- Filters invalid records

### Gold Layer
- Creates business-ready tables
- Computes KPIs including:
  - Gross Amount
  - Discount Amount
  - Net Amount
- Applies FX (Foreign Exchange) conversions
- Builds denormalized reporting tables for dashboards

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
              BI Dashboards
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

Creates the Unity Catalog objects:

- ecommerce catalog
- bronze schema
- silver schema
- gold schema

---

## 1_medallion_processing_dim

### 1_dim_bronze.py

Loads raw dimension datasets into Bronze tables.

Examples:

- Brands
- Categories
- Products
- Customers
- Date

---

### 2_dim_silver.py

Transforms Bronze dimension tables by:

- Removing duplicates
- Type casting
- Cleaning invalid values
- Standardizing formats

---

### 3_dim_gold.py

Builds Gold dimension tables such as:

- gld_dim_products
- gld_dim_customers
- gld_dim_category
- gld_dim_brand
- gld_dim_date

---

## 1_medallion_processing_fact

### 1_fact_bronze.py

Loads raw **order_items** transaction data into Bronze.

---

### 2_fact_silver.py

Processes transaction data by:

- Cleaning
- Deduplication
- Validation
- Standardization

---

### 3_fact_gold.py

Implements business logic including:

- Gross Amount
- Discount Amount
- Net Amount
- FX Currency Conversion

Creates the final Gold Fact table.

---

## 1_dashboard_code

### denormalise_table_query.txt

Contains the SQL query used to generate the final reporting table:

```
ecommerce.gold.fact_transactions_denorm
```

This denormalized table is optimized for BI tools like:

- Power BI
- Tableau
- Databricks SQL Dashboard

---

# Environment Configuration

The notebooks are designed to run inside Databricks.

A recommended configuration block:

```python
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("raw_data_path", "dbfs:/mnt/ecommerce/source_data/raw/")

ENV = dbutils.widgets.get("environment")
RAW_DATA_PATH = dbutils.widgets.get("raw_data_path")
```

This allows switching between development, testing, and production environments.

---

# Execution Order

Run the pipeline in the following order.

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

### Step 1

Run

```
setup_catalog.py
```

Creates the required catalog and schemas.

---

### Step 2

Run

```
1_dim_bronze.py
```

Loads raw dimension data.

---

### Step 3

Run

```
1_fact_bronze.py
```

Loads raw transaction data.

---

### Step 4

Run

```
2_dim_silver.py
```

Processes dimension tables.

---

### Step 5

Run

```
2_fact_silver.py
```

Processes fact tables.

---

### Step 6

Run

```
3_dim_gold.py
```

Creates Gold dimensions.

---

### Step 7

Run

```
3_fact_gold.py
```

Creates Gold fact tables with business metrics.

---

### Step 8

Execute

```
denormalise_table_query.txt
```

Creates the reporting view.

---

# Data Validation

Example SQL queries for monitoring the pipeline.

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

# Best Practices Implemented

### Explicit Schema Enforcement

- Uses `StructType`
- Prevents schema drift
- Improves data quality

---

### Auditability

Every Bronze record stores metadata including:

- Source file
- Ingestion timestamp

This makes debugging and lineage tracking easier.

---

### Schema Evolution

Tables are written using:

```python
.saveAsTable(...)
```

along with

```python
mergeSchema=True
```

allowing new columns to be added without breaking existing pipelines.

---

### Data Quality

The Silver layer performs:

- Null handling
- Duplicate removal
- Type validation
- Invalid record filtering
- Standardized formatting

---

### Business Transformation

The Gold layer computes:

- Gross Sales
- Discounts
- Net Sales
- Currency Conversion
- Reporting Tables

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

- Ingests raw eCommerce data
- Cleans and standardizes datasets
- Builds trusted dimensional and fact tables
- Computes business KPIs
- Generates analytics-ready reporting tables
- Supports BI dashboards using Delta Lake and Databricks SQL

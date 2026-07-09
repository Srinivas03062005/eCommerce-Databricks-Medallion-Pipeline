# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# from pyspark.sql import Row
# Imports the Row class.

# Row
# Represents a single row (record) in a PySpark DataFrame.

# Used to create DataFrames manually or for testing.

# Example:
# Row(id=1, name="Alice")



# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products 

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# createOrReplaceTempView() → Converts a DataFrame into a temporary SQL table (view).
# v_products, v_brands, v_category → Temporary view names used to run SQL queries, for example:
# SELECT * FROM v_products;

# COMMAND ----------

display(spark.sql("select * from v_products limit 5"))

# COMMAND ----------

display(spark.sql('select * from v_category limit 5'))

# COMMAND ----------

display(spark.sql('select * from v_brands limit 5'))

# COMMAND ----------


# Make sure we’re on the right catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Build brands×category mapping and write Gold table
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS (SELECT b.brand_name,b.brand_code,c.category_name,c.category_code FROM v_brands b INNER JOIN v_category c ON b.category_code = c.category_code)
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'Not Available') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available')   AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p.file_name,
# MAGIC   p.ingest_timestamp
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC   ON p.brand_code = bc.brand_code;
# MAGIC
# MAGIC
# MAGIC -- CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC -- Create the table and store the result of the SELECT query in it.
# MAGIC
# MAGIC
# MAGIC -- COALESCE(bc.category_name, 'Not Available') AS category_name,
# MAGIC -- If category_name is NULL, replace it with 'Not Available' and name the column category_name. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- 1. Join v_brands and v_category to create a temporary table (brands_categories).
# MAGIC -- 2. Join v_products with the temporary table using brand_code.
# MAGIC -- 3. Add brand_name and category_name to each product.
# MAGIC -- 4. If a brand_name or category_name is missing, replace it with 'Not Available'.
# MAGIC -- 5. Store the final merged product data in the Gold table : gold.gld_dim_products. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# India states
india_region = {
    "MH": "West",
    "GJ": "West",
    "RJ": "West",
    "KA": "South",
    "TN": "South",
    "TS": "South",
    "AP": "South",
    "KL": "South",
    "UP": "North",
    "WB": "North",
    "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast",
    "WA": "West",
    "NSW": "East",
    "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England",
    "WLS": "Wales",
    "NIR": "Northern Ireland",
    "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast",
    "FL": "South",
    "NJ": "NorthEast",
    "CA": "West", 
    "NY": "NorthEast",
    "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi",
    "DU": "Dubai",
    "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West",
    "AB": "West",
    "ON": "East",
    "QC": "East",
    "NS": "East",
    "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# ============================================
# COUNTRY & REGION MAPPING - NOTES
# ============================================

# india_region = {...}
# Creates a dictionary that maps Indian state codes to their regions.

# australia_region = {...}
# Creates a dictionary that maps Australian state codes to their regions.

# uk_region = {...}
# Creates a dictionary that maps UK state/country codes to their regions.

# us_region = {...}
# Creates a dictionary that maps US state codes to their regions.

# uae_region = {...}
# Creates a dictionary that maps UAE emirate codes to their emirates.

# singapore_region = {...}
# Creates a dictionary that maps Singapore's state code to its region.

# canada_region = {...}
# Creates a dictionary that maps Canadian province codes to their regions.

# Dictionary Syntax
# Key   : Value
# State Code : Region

# Example:
# "MH" : "West"
# "CA" : "West"
# "DU" : "Dubai"

# ============================================
# MASTER DICTIONARY
# ============================================

# country_state_map = {...}
# Combines all country dictionaries into one master dictionary.

# Key   -> Country Name
# Value -> Corresponding state-region dictionary

# Example:
# country_state_map["India"]
# Returns the India dictionary.

# Example:
# country_state_map["India"]["KA"]
# Returns "South"

# Example:
# country_state_map["United States"]["CA"]
# Returns "West"

# ============================================
# WHY IS THIS USED?
# ============================================

# Makes it easy to find the region of a state
# based on the country and state code.

# Avoids writing multiple if-else conditions.



# COMMAND ----------

# 1 Flatten country_state_map into a list of Rows
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]    

# rows = []
# Create an empty list.

# country_state_map.items()
# Returns each country and its state dictionary.

# states.items()
# Returns each state code and its region.

# Row(...)
# Creates a Spark Row object.

# country=country
# Stores the country name in the 'country' column.

# state=state_code
# Stores the state code in the 'state' column.

# region=region
# Stores the region name in the 'region' column.



# rows.append(...)
# Adds the Row object to the rows list.


# rows[:10]
# Displays the first 10 rows.
# List Slicing
# Used to access a specific portion of a list.

# Syntax : list_name[start:end]
# rows[:10] : Returns the first 10 elements from the rows list.

# rows[:10]
# Returns elements from the beginning (index 0) up to index 9.
# Nothing before ':' means start from index 0.


# COMMAND ----------

# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)

# Optional: show mapping
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})
# Replace NULL values in the region column with 'Other'.

display(df_gold.limit(500))

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date/Calendar

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
# F.col("date")
# Selects the date column.
# Datatype: Date

# F.date_format(F.col("date"), "yyyyMMdd")
# Converts the date to the format YYYYMMDD.
# Example: 2025-07-07 → "20250707"
# Datatype: String

# .cast("int")
# Converts the formatted date from String to Integer.
# Example: "20250707" → 20250707
# Datatype: Integer

# withColumn("date_id", ...)
# Creates a new Integer column named date_id.




# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
# withColumn("month_name", ...)
# Creates a new column named month_name.

# F.col("date")
# Selects the date column.
# Datatype: Date

# F.date_format(F.col("date"), "MMMM")
# Extracts the month from the date in the specified format.
# Datatype: String

# Month Format Patterns:
# "M"    -> Month number (1, 2, ..., 12)
# "MM"   -> Two-digit month number (01, 02, ..., 12)
# "MMM"  -> Short month name (Jan, Feb, Mar, ...)
# "MMMM" -> Full month name (January, February, March, ...)

# Example:
# Date: 2025-01-15
# "M"    -> 1
# "MM"   -> 01
# "MMM"  -> Jan
# "MMMM" -> January

# month_name
# Stores the extracted month name as a String.



# Add is_weekend column
df_gold = df_gold.withColumn("is_weekend",F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0))
# Creates an is_weekend column by assigning 1 to Saturday/Sunday and 0 to all other days.


display(df_gold.limit(5))


# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

# write table to gold layer
df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED ecommerce.gold.gld_dim_date; 
# MAGIC
# MAGIC
# MAGIC -- DESCRIBE EXTENDED
# MAGIC -- Displays the schema and detailed metadata of a table.
# MAGIC
# MAGIC -- ecommerce
# MAGIC -- Catalog name.
# MAGIC
# MAGIC -- gold
# MAGIC -- Schema (database) name.
# MAGIC
# MAGIC -- gld_dim_date
# MAGIC -- Table name.
# MAGIC
# MAGIC -- Output includes:
# MAGIC -- Column names
# MAGIC -- Data types
# MAGIC -- Table location
# MAGIC -- Storage format (e.g., Delta)
# MAGIC -- Other metadata
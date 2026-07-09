# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F 


# pyspark                 ← Package (Main PySpark library)
# │
# └── sql                 ← Subpackage (Contains Spark SQL components)
#     │
#     ├── types           ← Module (Contains classes for defining DataFrame schemas and data types)
#     │   ├── StructType        ← Class (Defines the complete DataFrame schema by grouping multiple columns)
#     │   ├── StructField       ← Class (Defines a single column in the schema, including its name, data type, and nullability)
#     │   ├── StringType        ← Data Type Class (Represents string/text values)
#     │   ├── IntegerType       ← Data Type Class (Represents integer values)
#     │   ├── DateType          ← Data Type Class (Represents date values in YYYY-MM-DD format)
#     │   ├── TimestampType     ← Data Type Class (Represents date and time values)
#     │   └── FloatType         ← Data Type Class (Represents floating-point/decimal numbers)
#     │
#     └── functions       ← Module (Contains built-in Spark SQL functions for DataFrame operations)
#         ├── col()              ← Function (Returns a reference to a DataFrame column)
#         ├── upper()            ← Function (Converts a string to uppercase)
#         ├── current_date()     ← Function (Returns the current system date)
#         └── current_timestamp()← Function (Returns the current system date and timestamp)


# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands

# COMMAND ----------

# Define schema 
brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])   

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"

df = spark.read.option('header', "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

# add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(5))   


# raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"
# ├── raw_data_path      ← Variable (Stores the path to the source CSV files)
# └── "/Volumes/.../*.csv" ← String (Path to all CSV files using a wildcard *)

# df = spark.read
# ├── spark              ← SparkSession Object (Entry point to Spark)
# ├── read               ← DataFrameReader Object (Used to read data from files)
# ├── option()           ← Method (Sets a read option)
# │   ├── header="true"  ← Reads the first row as column names
# │   └── delimiter=","  ← Specifies that values are separated by commas
# ├── schema()           ← Method (Applies the predefined schema: brand_schema)
# ├── csv()              ← Method (Reads CSV files into a DataFrame)
# └── df                 ← DataFrame (Stores the loaded data)

# Add metadata columns
# df = df.withColumn(...)
# ├── withColumn()       ← Method (Adds or replaces a column)
# ├── "_source_file"     ← New column name
# ├── F.col()            ← Function (Gets the value of an existing column)
# ├── "_metadata.file_path" ← Metadata column containing the source file path
# ├── "ingested_at"      ← New column name
# └── F.current_timestamp() ← Function (Adds the current timestamp)

# display(df.limit(5))
# ├── limit(5)           ← Method (Returns the first 5 rows)
# └── display()          ← Databricks Function (Displays the DataFrame as a table)






# ==============================
# StructType & StructField Notes
# ==============================

# 1. StructType
# - StructType is a class.
# - It represents a structured object (a collection of fields).
# - It is used to define:
#     a) The schema of an entire DataFrame.
#     b) The data type of a single struct column.

# 2. StructField
# - StructField is a class.
# - It defines one field (column) inside a StructType.
# - A StructField contains:
#     • Column name
#     • Data type
#     • Nullable (True/False)

# Example: DataFrame Schema
    # brand_schema = StructType([
    #    StructField("brand_code", StringType(), True),
    #    StructField("brand_name", StringType(), True),
    #    StructField("category_code", StringType(), True)
    # ])

# Here:
# StructType   -> Schema of the DataFrame
# StructField  -> Each column in the DataFrame


# ==============================
# _metadata Notes
# ==============================

# - _metadata is a hidden column automatically created by Spark/Databricks.
# - It is NOT stored inside the CSV file.
# - It is a StructType (struct) column.

# Structure:

# DataFrame
# │
# ├── brand_code      (StringType)
# ├── brand_name      (StringType)
# ├── category_code   (StringType)
# └── _metadata       (StructType)
#       ├── file_path
#       ├── file_name
#       ├── file_size
#       └── modification_time

# Important:
# - _metadata is ONE column.
# - file_path, file_name, file_size are fields inside that column.
# - They are NOT separate DataFrame columns.
# - They are NOT another table.

# Accessing a field:

# F.col("_metadata.file_path")

# Dot notation means:
# _metadata  -> Struct column
# file_path  -> Field inside the struct


# Example:

# df = df.withColumn(
#     "_source_file",
#     F.col("_metadata.file_path")
# )

# Result:
# Creates a new normal column called "_source_file"
# containing the source file path for every row.

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_brands")



# ==============================
# mergeSchema Notes
# ==============================

# mergeSchema
# - A Delta Lake write option.
# - Used to automatically merge the DataFrame schema with the existing Delta table schema.
# - Prevents schema mismatch errors when new columns are added.
# - Commonly used during schema evolution.

# Example:

# Existing Table
# id
# name

# New DataFrame
# id
# name
# age

# Without mergeSchema
#  Schema mismatch error

# With mergeSchema = true
#  Final table schema:
# id
# name
# age

# Interview Point:
# mergeSchema allows Delta Lake to automatically update the table schema
# by adding new columns from the DataFrame.






# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_category)
df_raw.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_category")               

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  #datatype is string due to incoming data contain anamolies
    StructField("length_cm", StringType(), True),     #datatype is string due to incoming data contain anamolies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

# ==========================================
# Why use StringType for Raw/Incoming Data?
# ==========================================

# Anomaly:
# - An anomaly is an invalid, unexpected, or incorrect value in the source data.

# Examples of anomalies:
# Expected Age      Incoming Value
# ------------      --------------
# 25                abc
# 30                N/A
# 40                ""
# 50                12A

# Why use StringType?
# - Raw data from CSV/JSON may contain anomalies.
# - Reading all columns as StringType ensures Spark can read every record.
# - This prevents data type conversion errors during ingestion.
# - After validation and cleaning, valid values are cast to their correct data types.

# Example:

# Raw Data
# age
# ---
# 25
# 30
# abc

# Read as IntegerType
#   Error or invalid value because "abc" is not an integer.

# Read as StringType
#    All values are read successfully:
# "25"
# "30"
# "abc"

# Later (Silver Layer)
# - Validate the data.
# - Remove or reject invalid values.
# - Convert valid values to IntegerType.

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path).withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_products)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_products")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load data using the schema defined
raw_data_path ="/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path).withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_customers)
df_raw.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_customers")      

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

# COMMAND ----------


# Define schema for the data file
date_schema = StructType([
    StructField("date", StringType(), True),           # Raw date in string format
    StructField("year", IntegerType(), True),          # Year
    StructField("day_name", StringType(), True),       # Day name (can be mixed case)
    StructField("quarter", IntegerType(), True),       # Quarter
    StructField("week_of_year", IntegerType(), True),  # Week of year (can be negative)
])

# Load data using the schema defined
raw_data_path = f"/Volumes/ecommerce/source_data/raw/date/*.csv" 

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_calendar) 
df_raw.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_calendar")               

# COMMAND ----------


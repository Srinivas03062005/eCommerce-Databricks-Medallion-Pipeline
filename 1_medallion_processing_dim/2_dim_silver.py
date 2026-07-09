# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleaning and Transformation for Dimension Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brands

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))

# F.trim()
# ├── Function
# ├── Removes leading spaces.
# ├── Removes trailing spaces.
# └── Does NOT remove spaces between words.

df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
df_silver.show(10) 


# F.regexp_replace(column, pattern, replacement)
# Searches the column using a regular expression (regex).
# Replaces matching characters with the specified replacement.

# ^ has two meanings in Regular Expressions (Regex):

# 1. Outside [] (at the beginning)
# ^abc
# - Means "starts with".
# - Example:
#   ^A → String starts with 'A'.

# 2. Inside [] (as the first character)
# [^abc]
# - Means "NOT".
# - Matches any character except the ones listed.
# - Example:
#   [^A-Za-z0-9]
#   → Matches any character that is NOT a letter or a number.



# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

# Anomalies dictionary
anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

# PySpark replace is easy
df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])

# Show results
df_silver.select("category_code").distinct().show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_bronze.show(10)

# COMMAND ----------

df_duplicates = df_bronze.groupBy("category_code").count().filter(F.col("count") > 1)
display(df_duplicates) 

# groupBy()
# ├── Method
# ├── Groups rows based on one or more columns.
# └── Used before aggregation functions.

# count()
# ├── Aggregation Method
# ├── Counts the number of rows in each group.
# ├── Automatically creates a new column named "count".
# └── Can be renamed using withColumnRenamed().

# filter()
# ├── Method
# ├── Filters rows based on a condition.
# └── Here, keeps only groups where count > 1.

# F.col("count")
# └── Refers to the automatically created "count" column.

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_category") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Check `weight_grams` (contains 'g')

# COMMAND ----------

# Check weight_grams column
df_bronze.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# replace 'g' with ''
df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)


# cast(IntegerType())
# ├── Method
# ├── Converts the StringType column to IntegerType.
# └── Used after cleaning the values.


# COMMAND ----------

# MAGIC %md
# MAGIC Check `length_cm` (comma instead of dot)

# COMMAND ----------

df_silver.select("length_cm").show(3)

# COMMAND ----------

# replace , with .
df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC `category_code` and `brand_code` are in lower case. we need to make it all upper case

# COMMAND ----------

df_silver.select("category_code", "brand_code").show(2)

# COMMAND ----------

# convert category_code and brand_code to upper case
df_silver = df_silver.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
).withColumn(
    "brand_code",
    F.upper(F.col("brand_code"))
)
df_silver.select("category_code", "brand_code").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Spelling mistakes in `material` column

# COMMAND ----------

df_silver.select("material").distinct().show()

# COMMAND ----------

# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)

# F.when()
# ├── Function
# ├── Works like an IF condition.
# ├── Checks a condition.
# └── If the condition is True, returns the specified value.

df_silver.select("material").distinct().show()    

# COMMAND ----------

# MAGIC %md
# MAGIC Negative values in `rating_count`

# COMMAND ----------

df_silver.filter(F.col('rating_count')<0).select("rating_count").show(3)


# COMMAND ----------

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(0)  # if null, replace with 0
)

# withColumn()
# ├── Method
# ├── Adds or replaces a column.
# └── Returns a new DataFrame.

# F.col()
# └── References the rating_count column.

# isNotNull()
# ├── Method
# └── Checks whether a column value is not null.

# F.when()
# ├── Function
# ├── Works like an IF condition.
# └── Executes when the condition is True.

# F.abs()
# ├── Function
# ├── Returns the absolute (positive) value.
# └── Converts negative numbers to positive.

# otherwise()
# ├── Method
# ├── Works like ELSE.
# └── Executes when the when() condition is False.

# COMMAND ----------

# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in `customer_id` column

# COMMAND ----------

null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

# There are 300 null values in customer_id column. Display some of those
df_bronze.filter(F.col("customer_id").isNull()).show(3)

# COMMAND ----------

# Drop rows where 'customer_id' is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver.count()
print(f"Row count after droping null values: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in `phone` column

# COMMAND ----------

null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 

# COMMAND ----------

df_silver.filter(F.col("phone").isNull()).show(3)

# COMMAND ----------

### Fill null values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver.filter(F.col("phone").isNull()).show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar/Date

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Converting String to Date

# COMMAND ----------

from pyspark.sql.functions import to_date


# Convert the string column to a date type
df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"], "dd-MM-yyyy"))

# COMMAND ----------

print(df_silver.printSchema())

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicates

# COMMAND ----------

# Find duplicate rows in the DataFrame
duplicates = df_silver.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# COMMAND ----------

# Remove duplicate rows
df_silver = df_silver.dropDuplicates(['date'])

# Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates: ", row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC `day_name` normalize casing

# COMMAND ----------

# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

# F.initcap()
# Converts the first letter of each word to uppercase.
# Converts all remaining letters to lowercase.

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert negative `week_of_year` to positive

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))  # Convert negative to positive

df_silver.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Enhance `quarter` and `week_of_year` column

# COMMAND ----------

df_silver = df_silver.withColumn(
    "quarter",
    F.format_string("Q%s-%s", F.col("quarter"), F.col("year"))
)

df_silver = df_silver.withColumn(
    "week_of_year",
    F.format_string("Week%s-%s", F.col("week_of_year"), F.col("year"))
)

df_silver.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns

# COMMAND ----------

# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_calendar")
# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df = spark.table(f'{catalog_name}.bronze.brz_order_items')
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Transformation: Drop any duplicates
df = df.dropDuplicates(["order_id", "item_seq"])
# dropDuplicates(["order_id", "item_seq"])
# Removes duplicate rows by checking the combination of
# order_id and item_seq.
# If multiple rows have the same values for both
# order_id and item_seq, Spark keeps one row
# and removes the remaining duplicate rows.




# Transformation: Convert 'Two' → 2 and cast to Integer
df = df.withColumn("quantity",F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int"))

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df = df.withColumn("unit_price",F.regexp_replace("unit_price", "[$]", "").cast("double"))

# Transformation : Remove '%' from discount_pct and cast to double
df = df.withColumn("discount_pct",F.regexp_replace("discount_pct", "%", "").cast("double"))

# Transformation : coupon code processing (convert to lower)
df = df.withColumn("coupon_code", F.lower(F.trim(F.col("coupon_code"))))
# F.col("coupon_code")
# Selects the coupon_code column.

# F.trim(...)
# Removes leading and trailing spaces.
# Example: " SAVE10 " → "SAVE10"

# F.lower(...)
# Converts the text to lowercase.
# Example: "SAVE10" → "save10"



# Transformation : channel processing 
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel"))
)

# COMMAND ----------


# Transformation: datatype conversions
# 1) Convert dt (string → date)
df = df.withColumn("dt",F.to_date("dt", "yyyy-MM-dd"))


# 2) Convert order_ts (string → timestamp)
df = df.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
    )
)
# withColumn("order_ts", ...)
# Updates the order_ts column.

# F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss")
# Converts the string to Timestamp using the format YYYY-MM-DD HH:MM:SS.
# Example: 2025-08-01 22:53:52

# F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")
# Converts the string to Timestamp using the format DD-MM-YYYY HH:MM.
# Example: 01-08-2025 22:53

# F.coalesce(...)
# Returns the first non-NULL Timestamp.
# If the first format doesn't match, Spark tries the second format.

# order_ts
# Stores the converted Timestamp value.

# F.coalesce(...)
# Returns the first non-NULL Timestamp.
# If none of the formats match, the result is NULL.




# 3) Convert item_seq (string → integer)
df = df.withColumn("item_seq",F.col("item_seq").cast("int"))


# 4) Convert tax_amount (string → double, strip non-numeric characters)
df = df.withColumn("tax_amount",F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double"))
# F.regexp_replace("tax_amount", r"[^0-9.\-]", "")
# Searches the tax_amount column and removes all characters
# except digits (0-9), decimal point (.), and minus sign (-).

# r"..."
# Raw string.
# Used to write regular expressions without treating '\' as
# a special escape character.

# [ ]
# Character set.
# Specifies the characters to match.

# ^
# Inside [ ], '^' means NOT.
# Matches any character that is NOT in the character set.

# 0-9
# Matches any digit from 0 to 9.

# .
# Matches the decimal point (.).

# \-
# Matches the minus sign (-).
# '\' is used to treat '-' as a literal character.

# [^0-9.\-]
# Matches every character except:
# - Digits (0-9)
# - Decimal point (.)
# - Minus sign (-)

# ""
# Replaces the matched characters with an empty string,
# effectively removing them.

# Examples:
# "$123.45"   -> "123.45"
# "₹250.75"   -> "250.75"
# "USD 75.50" -> "75.50"
# "-45.80"    -> "-45.80" (minus sign is preserved)

# Why is this used?
# Cleans the tax_amount column by removing currency symbols,
# letters, spaces, and other unwanted characters before
# converting it to a numeric datatype.



#Transformation : Add processed time 
df = df.withColumn("processed_time", F.current_timestamp())

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# check the final datatypes
df.printSchema()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_order_items")
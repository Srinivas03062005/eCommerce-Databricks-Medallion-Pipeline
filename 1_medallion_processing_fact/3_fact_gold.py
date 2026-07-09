# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.slv_order_items")

df.limit(10).display()

# COMMAND ----------

# Example :- 
# ----------

# Quantity = 2
# Unit Price = ₹500
# Discount = 10%
# Tax (GST) = 18%


# Step 1: Calculate Gross Amount
# Gross Amount = Quantity × Unit Price
#              = 2 × 500
#              = ₹1000
# This is the original price before any discount or tax.


# Step 2: Calculate Discount
# Discount = 10% of ₹1000
#          = ₹100


# Step 3: Calculate Sale Amount (Taxable Amount)
# Sale Amount = Gross Amount − Discount
#             = ₹1000 − ₹100
#             = ₹900


# Step 4: Calculate Tax (GST)
# GST = 18% of ₹900
#     = ₹162


# Step 5: Calculate Final Amount
# Final Amount = Sale Amount + GST
#              = ₹900 + ₹162
#              = ₹1062



# Flow :- 
# -------

#    Quantity × Unit Price
#         ↓
#    Gross Amount (₹1000)
#         ↓
#    − Discount (₹100)
#         ↓
#    Sale Amount (₹900)
#         ↓
#    + GST (₹162)
#         ↓
#    Final Amount (₹1062)

# Remember:
# Gross Amount = Before discount and tax.
# Sale Amount = After discount, before tax.
# Final Amount = After adding tax (the amount the customer actually pays).




# 1) Gross Amount
df = df.withColumn("gross_amount",F.col("quantity") * F.col("unit_price"))

# 2) Discount
df = df.withColumn("discount_amount",F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0)))

# 3) Final Amount
df = df.withColumn("sale_amount",F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))

# add date id
df = df.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df = df.withColumn("coupon_flag",F.when(F.col("coupon_code").isNotNull(),1).otherwise(0))

df.limit(5).display()     




# COMMAND ----------

# MAGIC %md
# MAGIC currency conversion

# COMMAND ----------

# --- 1) Define your fixed FX rates (as of 2025-10-15, like your PBI note) ---
fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}

rates = [(k, float(v)) for k, v in fx_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()



# ---------------------------------------------
# Currency Conversion 
# ---------------------------------------------

# Step 1: Create a Python dictionary to store exchange rates.
# A dictionary stores data as Key : Value pairs.

# Key   -> Currency code (USD, GBP, INR, etc.)
# Value -> Exchange rate in INR

# Example:
# "USD": 88.29 means
# 1 USD = ₹88.29

# "GBP": 117.98 means
# 1 GBP = ₹117.98


# ---------------------------------------------
# Step 2: Convert the dictionary into a list.
# ---------------------------------------------

# fx_rates.items() returns all key-value pairs.

# Output:
# ("INR", 1.00)
# ("AED", 24.18)
# ("AUD", 57.55)
# ...

# for k, v in fx_rates.items()
# k -> Currency code
# v -> Exchange rate

# float(v) converts the exchange rate into float datatype.

# After executing:
# rates = [(k, float(v)) for k, v in fx_rates.items()]

# rates becomes:
# [
#   ("INR", 1.0),
#   ("AED", 24.18),
#   ("AUD", 57.55),
#   ("CAD", 62.93),
#   ("GBP", 117.98),
#   ("SGD", 68.18),
#   ("USD", 88.29)
# ]

# The variable rates is a list of tuples.



# ---------------------------------------------
# Step 3: Create a Spark DataFrame.
# ---------------------------------------------

# spark.createDataFrame(data, column_names)

# data         -> List of tuples
# column_names -> Names of the DataFrame columns

# Here,
# currency -> Currency code
# inr_rate -> Exchange rate in INR



# The DataFrame looks like:

# +--------+---------+
# |currency|inr_rate |
# +--------+---------+
# |INR     |1.00     |
# |AED     |24.18    |
# |AUD     |57.55    |
# |CAD     |62.93    |
# |GBP     |117.98   |
# |SGD     |68.18    |
# |USD     |88.29    |
# +--------+---------+


# ---------------------------------------------
# Step 4: Display the DataFrame.
# ---------------------------------------------

# show() displays the DataFrame on the screen.



# ---------------------------------------------
# Why do we create this DataFrame?
# ---------------------------------------------

# Suppose a sale amount is stored in INR.

# Sale Amount = ₹8829
# Currency    = USD

# Exchange Rate:
# 1 USD = ₹88.29

# USD Amount = 8829 / 88.29
#            = 100 USD

# Similarly,

# GBP Amount = INR Amount / 117.98
# AED Amount = INR Amount / 24.18
# AUD Amount = INR Amount / 57.55

# This DataFrame is later joined with the sales DataFrame
# to convert sale amounts from INR into different currencies. 


# COMMAND ----------

df = (
    df.join(rates_df,rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),"left")
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
) 


# -------------------------------------------------------
# Currency Conversion using Exchange Rates - Notes
# -------------------------------------------------------


# -------------------------------------------------------
# Step 1: Join the sales DataFrame with the exchange rates DataFrame.
# -------------------------------------------------------

# join() combines two DataFrames based on a matching condition.

# Here,
# Left DataFrame  -> df (Sales Data)
# Right DataFrame -> rates_df (Currency Exchange Rates)


# Join Condition:
# rates_df.currency == F.upper(F.trim(F.col("unit_price_currency")))

# This means:
# Compare the "currency" column from rates_df
# with the "unit_price_currency" column from df.



# -------------------------------------------------------
# Why use trim()?
# -------------------------------------------------------

# trim() removes extra spaces.

# Example:

# Before trim:
# " USD "

# After trim:
# "USD"

# -------------------------------------------------------
# Why use upper()?
# -------------------------------------------------------

# upper() converts the text to uppercase.

# Example:

# "usd"  -> "USD"
# "Usd"  -> "USD"
# "uSd"  -> "USD"

# This ensures the values match correctly during the join.


# -------------------------------------------------------
# Why use a Left Join?
# -------------------------------------------------------

# "left" means:

# Keep ALL rows from the sales DataFrame (df).

# If a matching currency exists in rates_df,
# add its exchange rate.

# If no match exists,
# the exchange rate becomes NULL.

# Example:
# Sales Data (df)
# +-----------+---------------------+
# |sale_amount|unit_price_currency  |
# +-----------+---------------------+
# |100        |USD                  |
# |200        |GBP                  |
# |300        |XYZ                  |
# +-----------+---------------------+

# Exchange Rates (rates_df)

# +--------+---------+
# |currency|inr_rate |
# +--------+---------+
# |USD     |88.29    |
# |GBP     |117.98   |
# +--------+---------+


# After Left Join
# +-----------+---------------------+---------+
# |sale_amount|unit_price_currency  |inr_rate |
# +-----------+---------------------+---------+
# |100        |USD                  |88.29    |
# |200        |GBP                  |117.98   |
# |300        |XYZ                  |NULL     |
# +-----------+---------------------+---------+



# -------------------------------------------------------
# Step 2: Convert Sale Amount into INR.
# -------------------------------------------------------

# withColumn() creates a new column called "sale_amount_inr".

# Formula:
# sale_amount_inr = sale_amount × inr_rate

# Example:
# sale_amount = 100 USD
# inr_rate    = 88.29

# sale_amount_inr
# = 100 × 88.29
# = ₹8829


# -------------------------------------------------------
# Step 3: Round up the INR Amount.
# -------------------------------------------------------

# F.ceil() always rounds the value UP to the nearest integer.
# Examples:
# 8829.10 -> 8830
# 8829.99 -> 8830
# 8829.00 -> 8829

# Final column:
# sale_amount_inr
# contains the rounded INR amount.


# COMMAND ----------

df.limit(5).display()    

# COMMAND ----------

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

orders_gold_df.limit(5).display()

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_fact_order_items)
orders_gold_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity Check

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {catalog_name}.gold.gld_fact_order_items").show()
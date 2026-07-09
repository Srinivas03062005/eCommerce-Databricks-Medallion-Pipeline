# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC show databases from ecommerce;

# COMMAND ----------

# %sql
# DROP CATALOG IF EXISTS ecommerce CASCADE;

# DROP CATALOG → Deletes the ecommerce catalog.
# IF EXISTS → Deletes it only if it exists (avoids an error).
# CASCADE → Deletes the catalog and everything inside it (schemas, tables, views, etc.).


# COMMAND ----------


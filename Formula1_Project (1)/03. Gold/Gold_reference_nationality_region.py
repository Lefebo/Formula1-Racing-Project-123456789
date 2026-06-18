# Databricks notebook source
# MAGIC %md
# MAGIC # Nationality Reference Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE formula1.gold.nationality_region
# MAGIC (
# MAGIC     nationality STRING,
# MAGIC     continent STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into formula1.gold.nationality_region 
# MAGIC VALUES
# MAGIC ('Italian', 'Europe'),
# MAGIC ('German', 'Europe'),
# MAGIC ('British', 'Europe'),
# MAGIC ('American', 'North America'),
# MAGIC ('Indian', 'Asia'),
# MAGIC ('New Zealander', 'Oceania'),
# MAGIC ('French', 'Europe'),
# MAGIC ('South African', 'Africa'),
# MAGIC ('Russian', 'Europe'),
# MAGIC ('Swiss', 'Europe'),
# MAGIC ('Japanese', 'Asia'),
# MAGIC ('Malaysian', 'Asia'),
# MAGIC ('Belgian', 'Europe'),
# MAGIC ('Dutch', 'Europe'),
# MAGIC ('Brazilian', 'South America'),
# MAGIC ('Austrian', 'Europe'),
# MAGIC ('Canadian', 'North America'),
# MAGIC ('Australian', 'Oceania'),
# MAGIC ('Rhodesian', 'Africa'),
# MAGIC ('East German', 'Europe'),
# MAGIC ('Mexican', 'North America'),
# MAGIC ('Hong Kong', 'Asia'),
# MAGIC ('Spanish', 'Europe'),
# MAGIC ('Irish', 'Europe');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.nationality_region

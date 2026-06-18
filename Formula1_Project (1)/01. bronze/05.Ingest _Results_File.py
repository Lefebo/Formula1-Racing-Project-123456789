# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Results.json file
# MAGIC 1. Read the file file from landing zone
# MAGIC 2. Enriched the data with audit columns:
# MAGIC - Source File Name
# MAGIC - Ingestion Timestamp
# MAGIC
# MAGIC 3. write to delta table format

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------

# MAGIC %run ../00-common/02.Create_helper_function

# COMMAND ----------

source_file_name = f'{landing_folder_path}/results'
table_name = f'{catalog_name}.{bronze_schema}.Results'

# COMMAND ----------

source_file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the file

# COMMAND ----------


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType
)

schema = StructType([
    StructField("constructorId", StringType()),
    StructField("date", DateType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("status", StringType()),
    StructField("url", StringType())
])


# COMMAND ----------

resu_df = (
spark.read
     .format('json')
    .option('header', 'true')
    .option('mode','FAILFAST')
  .schema(schema)     
    .load(source_file_name)

)

# COMMAND ----------

display(resu_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
results_df = enriched_metadata(resu_df)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
      results_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC    season, 
# MAGIC    count(*) 
# MAGIC from formula1.bronze.results
# MAGIC    group by season
# MAGIC    

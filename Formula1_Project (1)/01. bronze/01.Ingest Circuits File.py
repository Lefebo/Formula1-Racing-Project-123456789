# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuits.csv file
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

source_file_name = f'{landing_folder_path}/circuits.csv'
table_name = f'{catalog_name}.{bronze_schema}.circuts'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DoubleType
cir_schema = StructType(
    [
        StructField('circuitId', StringType()), 
        StructField('url', StringType()),
        StructField('circutName', StringType()),
        StructField('lat',  DoubleType()),
        StructField('long',  DoubleType()),
        StructField('locality', StringType()),
        StructField('country', StringType())
    ]
)

# COMMAND ----------

cir_df = (
spark.read
    .format('csv')
    .option('header', 'true')
    .option('mode','FAILFAST')
    .schema(cir_schema)       
    .load(source_file_name)

)

# COMMAND ----------

display(cir_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
circuit_df = enriched_metadata(cir_df)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
    circuit_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers.csv file
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

source_file_name = f'{landing_folder_path}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.Constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType,DateType
constructors_schema = """constructorId string,
name string,
nationality string,
url string
"""


# COMMAND ----------

const_df = (
spark.read
     .format('json')
    .option('header', 'true')
    .option('mode','FAILFAST')
    .schema(constructors_schema)     
    .load(source_file_name)

)

# COMMAND ----------

display(const_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
constructors_df = enriched_metadata(const_df)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
      constructors_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

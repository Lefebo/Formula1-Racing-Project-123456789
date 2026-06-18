# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Races.csv file
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

source_file_name = f'{landing_folder_path}/races.csv'
table_name = f'{catalog_name}.{bronze_schema}.races'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType,DateType
race_schema = StructType(
    [
        StructField('season', IntegerType()), 
        StructField('round', IntegerType()), 
        StructField('url', StringType()), 
        StructField('raceName', StringType()), 
        StructField('date', DateType()), 
        StructField('circuitId', StringType())
    ]
)


# COMMAND ----------

races_df = (
spark.read
     .format('csv')
    .option('header', 'true')
    .option('mode','FAILFAST')
    .schema(race_schema)     
    .load(source_file_name)

)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
races_df = enriched_metadata(races_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
    races_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

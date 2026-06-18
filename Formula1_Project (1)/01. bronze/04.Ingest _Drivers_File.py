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

source_file_name = f'{landing_folder_path}/drivers.json'
table_name = f'{catalog_name}.{bronze_schema}.Drivers'

# COMMAND ----------

source_file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType,DateType
name_schema = StructType(
    [
        StructField('givenName', StringType()),
        StructField('familyName', StringType())
        
    ]
)

driver_schema = StructType(
    [
      StructField('driverId', StringType()),
        StructField('name', name_schema),
        StructField('dateOfBirth', DateType()), 
        StructField('nationality', StringType()),
        StructField('url',StringType())
    ]
)


# COMMAND ----------

driv_df = (
spark.read
     .format('json')
    .option('header', 'true')
    .option('mode','FAILFAST')
   .schema(driver_schema)     
    .load(source_file_name)

)

# COMMAND ----------

display(driv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
drivers_df = enriched_metadata(driv_df)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
      drivers_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

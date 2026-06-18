# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Sprints.json file
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

source_file_name = f'{landing_folder_path}/sprints'
table_name = f'{catalog_name}.{bronze_schema}.Sprints'

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

sprint_schema = StructType([
 
  StructField("date", DateType()),
  StructField("raceName", StringType()),
   StructField("round", IntegerType()),
   StructField("season", IntegerType()),
   StructField("url", StringType()),
   StructField("constructorId", StringType()),
   StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())  
])


# COMMAND ----------

sprints_df = (
spark.read
     .format('json')
    .option('header', 'true')
    .option('mode','FAILFAST')
    .option('multiLine',True)
    .schema(sprint_schema)     
    .load(source_file_name)

)

# COMMAND ----------

sprints_df.printSchema()

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Enriched the data with audit columns:

# COMMAND ----------

from pyspark.sql import functions as F
sprint_df = enriched_metadata(sprints_df)

# COMMAND ----------

display(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data

# COMMAND ----------

(
      sprint_df
       .write
       .mode("overwrite")
       .format("delta")
       .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC    driverID,
# MAGIC    count(*)
# MAGIC from (
# MAGIC select * from formula1.bronze.Sprints
# MAGIC where season == 2023
# MAGIC )
# MAGIC group by driverID
# MAGIC
# MAGIC    
# MAGIC    

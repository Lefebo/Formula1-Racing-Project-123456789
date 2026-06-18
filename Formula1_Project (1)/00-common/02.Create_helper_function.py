# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

def enriched_metadata(df):
    return (df.withColumn("Timestamp",F.current_timestamp())
     .withColumn("filePath",F.col("_metadata.file_path"))
     .withColumn("fileName",F.col("_metadata.file_name"))
     )

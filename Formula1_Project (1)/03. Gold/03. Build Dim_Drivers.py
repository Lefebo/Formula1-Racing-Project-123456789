# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim_Drivers
# MAGIC 1. Read Silver Drivers table
# MAGIC 2. Read gold ref_nationality_region table
# MAGIC 3. Join the data from Drivers with nationality_region using nationality
# MAGIC 4. Select the required columns
# MAGIC -  driver.driver_id
# MAGIC -  driver.driver_name
# MAGIC -  driver.date_of_birth
# MAGIC -  driver.nationality
# MAGIC -  nationality_region.Continent
# MAGIC
# MAGIC 5.write tranformed data to gold  Dim_Drivers table

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------

target_table =f'{catalog_name}.{gold_schema}.dim_drivers'

# COMMAND ----------

target_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_df =spark.table(f'{catalog_name}.{silver_schema}.drivers')


# COMMAND ----------

display(drivers_df.select('nationality').distinct())

# COMMAND ----------

nationality_region_df = spark.table(f'{catalog_name}.{gold_schema}.nationality_region')

# COMMAND ----------

dim_drivers = (
        (
     drivers_df
             .join(nationality_region_df, on="nationality", how='left')
             )
        .select(
                F.col('driver_id'),
                F.col('driver_name'),
                F.col('date_of_birth'),
                F.col('nationality'),
                F.col('continent').alias('nationality_region')
               )
        
)

# COMMAND ----------

display(dim_drivers)

# COMMAND ----------

(dim_drivers
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.dim_drivers
# MAGIC where nationality_region is not null and nationality_region == 'Europe'

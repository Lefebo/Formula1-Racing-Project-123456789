# Databricks notebook source
# MAGIC %md
# MAGIC # Build Constructor Dimension
# MAGIC 1. Read Silver constructor table
# MAGIC 2. Read gold ref_nationality_region table
# MAGIC 3. Join the data from constructors with ref_nationality_region using nationality
# MAGIC 4. Select the required columns
# MAGIC -  constructor.constructors_id
# MAGIC -  constructor.constructors_name
# MAGIC -  constructor.nationality
# MAGIC -  constructor.nationality_region
# MAGIC
# MAGIC 5.write tranformed data to gold  dim_races table

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------

# MAGIC %run ../00-common/02.Create_helper_function

# COMMAND ----------

target_table =f'{catalog_name}.{gold_schema}.dim_constructors'

# COMMAND ----------

target_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_df =spark.table(f'{catalog_name}.{silver_schema}.constructors')


# COMMAND ----------

display(constructors_df.select('nationality').distinct())

# COMMAND ----------

nationality_region_df = spark.table(f'{catalog_name}.{gold_schema}.nationality_region')

# COMMAND ----------

dim_constructors = (
        (
     constructors_df
             .join(nationality_region_df, on="nationality", how='left')
             )
        .select(
                F.col('constructor_id'),
                F.col('constructor_name'),
                F.col('nationality'),
                F.col('continent').alias('nationality_region')
               )
        
)

# COMMAND ----------

display(dim_constructors)

# COMMAND ----------

(dim_constructors
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.dim_constructors
# MAGIC where nationality_region is not null and nationality_region == 'Europe'

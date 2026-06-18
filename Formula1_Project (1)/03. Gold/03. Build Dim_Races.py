# Databricks notebook source
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC 1. Read Silver races table
# MAGIC 2. Read silver circuits table
# MAGIC 3. Join the data from races with circuits using circuit_id
# MAGIC 4. Select the required columns
# MAGIC -  races.season
# MAGIC - -races.round
# MAGIC - -races.race_name
# MAGIC -  races.race_date
# MAGIC -  circuits.circuit_name
# MAGIC -  circuits.locality
# MAGIC -  circuits.country
# MAGIC
# MAGIC
# MAGIC   5.write tranformed data to gold  dim_races table

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------

# MAGIC %run ../00-common/02.Create_helper_function

# COMMAND ----------

target_table =f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

target_table

# COMMAND ----------

races_df =spark.table(f'{catalog_name}.{silver_schema}.races')


# COMMAND ----------

circuits_df = spark.table(f'{catalog_name}.{silver_schema}.circuits')

# COMMAND ----------

races_circ_df = (
 races_df.join(circuits_df,
                            on= races_df.circuit_id==circuits_df.circuit_id,
                            how='inner')
             .select(races_df.season,
                     races_df.round,
                     races_df.race_name,
                     races_df.race_date,
                     circuits_df.circuit_name,
                     circuits_df.locality,
                     circuits_df.country
             )
)

# COMMAND ----------

(races_circ_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

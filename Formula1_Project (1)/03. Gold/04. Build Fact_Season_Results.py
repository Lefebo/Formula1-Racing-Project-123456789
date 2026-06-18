# Databricks notebook source
# MAGIC %md
# MAGIC # Build Fact Season_result Table
# MAGIC 1. Read Silver season table
# MAGIC 2. Read silver sprints table
# MAGIC 3. Join the data horizontally with union
# MAGIC 4. Select the required columns
# MAGIC -     -season
# MAGIC -     -round
# MAGIC -     -constructor_id
# MAGIC -     -driver_id
# MAGIC -     -grid_position
# MAGIC -     -completed_laps
# MAGIC -     -car_number
# MAGIC -     -points
# MAGIC -     -final_position
# MAGIC -     -status
# MAGIC
# MAGIC 5. Create a custom columns session_type whether if it is from RACES table or Results table, we use literal value RACES, Constructor.
# MAGIC 6. Create custom columns is_win, is_podium, has_points
# MAGIC
# MAGIC -      -Is_win if final_position is 1
# MAGIC -      -Is_podium if final_position between 1 and 3
# MAGIC -      -Has_points if the driver points greater than 0
# MAGIC - 
# MAGIC 7.write transformed data to gold fact_session_results table

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------

target_table =f'{catalog_name}.{gold_schema}.fact_season_results'

# COMMAND ----------

target_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_df =(
    spark.table(f'{catalog_name}.{silver_schema}.results')
         .withColumn('session_type',F.lit('RACES'))
         .withColumn('is_win',F.col('finish_position')==1)
         .withColumn('is_podium',F.col('finish_position').between(1,3))
         .withColumn('has_points',F.col('points')>0)
         .select(
             'season',
             'round',
             'session_type',
             'constructor_id',
             'driver_id',
             F.col('grid').alias('grid_position'),
             F.col('laps').alias('completed_laps'),
             'car_number',
             'points',
             'finish_position',
             'finish_position_text',
             'status',
             'is_win',
             'is_podium',
             'has_points'
         )
         
        )


# COMMAND ----------

sprints_df =(
    spark.table(f'{catalog_name}.{silver_schema}.sprints')
         .withColumn('session_type',F.lit('SPRINTS'))
         .withColumn('is_win',F.col('finish_position')==1)
         .withColumn('is_podium',F.col('finish_position').between(1,3))
         .withColumn('has_points',F.col('points')>0)
         .select(
             'season',
             'round',
             'session_type',
             'constructor_id',
             'driver_id',
             F.col('grid').alias('grid_position'),
             F.col('laps').alias('completed_laps'),
             'car_number',
             'points',
             'finish_position',
             'finish_position_text',
             'status',
             'is_win',
             'is_podium',
             'has_points'
         )
         
        )

# COMMAND ----------

fact_season_results = (
 results_df.unionByName(sprints_df)
)

# COMMAND ----------

(fact_season_results
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC            
# MAGIC            d.driver_name,
# MAGIC            f.session_type,
# MAGIC            f.season,
# MAGIC            is_win
# MAGIC
# MAGIC from formula1.gold.fact_season_results  as f
# MAGIC inner join formula1.gold.dim_drivers as d
# MAGIC on f.driver_id = d.driver_id
# MAGIC where is_win = true and season = 2021
# MAGIC
# MAGIC
# MAGIC

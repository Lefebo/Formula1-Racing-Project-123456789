# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Results Data
# MAGIC 1. Read the Bronze data 
# MAGIC 2. Keep only the columns required for analytics (Drop URL column)
# MAGIC 3. Standardize and rename  column names using snake case ('constructorId'->'constructor_Id',driverId,driver_Id,poistionText->finish_position_text)
# MAGIC  and rename the columns to make them more meaningful (date->race_date,grid->grid_position,laps->laps_completed,number->car_number,position_>finish_position)
# MAGIC 4. Check and drop null values in the records  by using season,round,constructor_id, driver_id
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform column values of race_name to Title case
# MAGIC 8. Write the transformed data to silver table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------


bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"


# COMMAND ----------

bronze_table

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Data

# COMMAND ----------

spr_df = spark.read.table(bronze_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop Unrequired columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Url columns has been removed in following step

# COMMAND ----------

spr_df = spr_df.select(['constructorId',
 'date',
 'driverId',
 'grid',
 'laps',
 'number',
 'points',
 'position',
 'positionText',
 'raceName',
 'round',
 'season',
 'status',
 'Timestamp',
 'filePath',
 'fileName'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rename Columns

# COMMAND ----------

spr_df = (
spr_df.withColumnsRenamed(
        {
   "driverId":"driver_id",
   'constructorId':'constructor_Id',
   'raceName':'race_name',
   'number':'car_number',
   'position':'finish_position',
   'positionText':'finish_position_text',
   "Timestamp":"time_stamp",
   "filePath":"file_path",
   "fileName":"file_name"
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Handle Missing Values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 show null records

# COMMAND ----------

spr_df.filter(spr_df.season.isNull() | spr_df.round.isNull()| spr_df.constructor_Id.isNull()
                | spr_df.driver_id.isNull()).display()

# COMMAND ----------

sprint_df = (
spr_df.filter(f.col("constructor_Id").isNotNull() & f.col("driver_id").isNotNull() & f.col("season").isNotNull() & f.col("round").isNotNull())
)
display(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Duplicate values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Duplicate if Exist

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = sprint_df.groupBy(sprint_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Use Drop Duplicates Method in circuit_id

# COMMAND ----------

# MAGIC %md
# MAGIC The table do have composite keys season and round

# COMMAND ----------

sprint_df= sprint_df.dropDuplicates(['constructor_id','season','driver_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Check the duplicate records has been dropped or not

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = sprint_df.groupBy(sprint_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)

# COMMAND ----------

display(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Transform column values of circuit_name and locality to Title

# COMMAND ----------

# MAGIC %md
# MAGIC Here we need to change the columns value into title case, The function used for this scenarios are initcap Translate the first letter of each word to upper case in the sentence.

# COMMAND ----------

sprints_cleaned_df = (
    sprint_df
       .withColumn("race_name",F.initcap(F.col("race_name")))

)

# COMMAND ----------

display(sprints_cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write the cleaned and transformed data into silver table

# COMMAND ----------

silver_table

# COMMAND ----------

(
   sprints_cleaned_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

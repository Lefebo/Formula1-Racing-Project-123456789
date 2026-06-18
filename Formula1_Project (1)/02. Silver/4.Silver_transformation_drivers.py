# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Drivers Data
# MAGIC 1. Read the Bronze data 
# MAGIC 2. Keep only the columns required for analytics (Drop URL column)
# MAGIC 3. Standardize and rename  column names using snake case (driverId→ driver_id, dateOfBirth-> date_of_birth)
# MAGIC  and rename the columns to make them more meaningful (date->race_date)
# MAGIC 4. Concatenate name.giveName and name.familyName to create new columns driver_name and transform the value to title case
# MAGIC 5. Check and handle null values in the records 
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform column values of nationality  to Title case
# MAGIC 8. Write the transformed data to silver table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------


bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"


# COMMAND ----------

bronze_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Data

# COMMAND ----------

driv_df = spark.read.table(bronze_table)


# COMMAND ----------

driv_df.columns

# COMMAND ----------

display(driv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop Unrequired columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Url columns has been removed in following step

# COMMAND ----------

driv_df = driv_df.select(['driverId',
 'name',
 'dateOfBirth',
 'nationality',
 'Timestamp',
 'filePath',
 'fileName'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rename Columns

# COMMAND ----------

driv_df = (
driv_df.withColumnsRenamed(
        {
   "driverId":"driver_id",
   "dateOfBirth":"date_Of_Birth",
   "Timestamp":"time_stamp",
   "filePath":"file_path",
   "fileName":"file_name"
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create new columns called driver_name

# COMMAND ----------

# DBTITLE 1,Cell 16
driv_df = (
    driv_df
         .withColumn("driver_name",
                       F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
         .drop("name")
)

# COMMAND ----------

display(driv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Handle Missing Values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 show null records

# COMMAND ----------

driv_df.filter(driv_df.driver_id.isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Duplicate values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Duplicate if Exist

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = driv_df.groupBy(driv_df.columns)\
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

driv_df= driv_df.dropDuplicates(['driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Check the duplicate records has been dropped or not

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = driv_df.groupBy(driv_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)

# COMMAND ----------

display(driv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Transform column values of circuit_name and locality to Title

# COMMAND ----------

# MAGIC %md
# MAGIC Here we need to change the columns value into title case, The function used for this scenarios are initcap Translate the first letter of each word to upper case in the sentence.

# COMMAND ----------

driver_cleaned_df = (
    driv_df
       .withColumn("nationality",F.initcap(F.col("nationality")))

)

# COMMAND ----------

display(driver_cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write the cleaned and transformed data into silver table

# COMMAND ----------

silver_table

# COMMAND ----------

(
   driver_cleaned_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

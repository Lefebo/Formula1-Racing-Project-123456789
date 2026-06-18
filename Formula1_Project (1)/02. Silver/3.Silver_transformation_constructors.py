# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC 1. Read the Bronze data 
# MAGIC 2. Keep only the columns required for analytics (Drop URL column)
# MAGIC 3. Standardize and rename  column names using snake case (circuitId→ circuit_id, Name-> constructor_name)
# MAGIC  and rename the columns to make them more meaningful (date->race_date)
# MAGIC 4. Check and handle null valuues in the records 
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform column values of nationality  to Title case
# MAGIC 7. Write the transformed data to silver table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------


bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"


# COMMAND ----------

bronze_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Data

# COMMAND ----------

const_df = spark.read.table(bronze_table)


# COMMAND ----------

const_df.columns

# COMMAND ----------

display(const_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop Unrequired columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Url columns has been removed in following step

# COMMAND ----------

const_df = const_df.select(['constructorId',
 'name',
 'nationality',
 'Timestamp',
 'filePath',
 'fileName'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rename Columns

# COMMAND ----------

const_df = (
const_df.withColumnsRenamed(
        {
   "constructorId":"constructor_id",
   "name":"constructor_name",
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

const_df.filter(const_df.constructor_id.isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Duplicate values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Duplicate if Exist

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = const_df.groupBy(const_df.columns)\
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

const_df = const_df.dropDuplicates(['constructor_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Check the duplicate records has been dropped or not

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = const_df.groupBy(const_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)

# COMMAND ----------

display(const_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Transform column values of circuit_name and locality to Title

# COMMAND ----------

# MAGIC %md
# MAGIC Here we need to change the columns value into title case, The function used for this scenarios are initcap Translate the first letter of each word to upper case in the sentence.

# COMMAND ----------

constructor_cleaned_df = (
    const_df
       .withColumn("nationality",F.initcap(F.col("nationality")))

)

# COMMAND ----------

display(constructor_cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write the cleaned and transformed data into silver table

# COMMAND ----------

silver_table

# COMMAND ----------

(
    constructor_cleaned_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

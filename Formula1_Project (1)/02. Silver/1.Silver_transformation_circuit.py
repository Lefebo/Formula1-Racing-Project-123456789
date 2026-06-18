# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Transform Circuit Data
# MAGIC 1. Read the Bronze data 
# MAGIC 2. Keep only the columns required for analytics (Drop URL column)
# MAGIC 3. Standardize and rename  column names using snake case (circuitId→ circuit_id, circuitName → circuit_name)
# MAGIC  and rename the columns to make them more meaningful (lat → latitude, long→ longitude)
# MAGIC 4. Handle null valuues in the records where specially record with null circuit_id will be dropped
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform column values of circuit_name and locality to Title
# MAGIC 7. Write the transformed data to silver table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.Enivroment_config

# COMMAND ----------


bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"


# COMMAND ----------

bronze_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Data

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)


# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop Unrequired columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Url columns has been removed in following step

# COMMAND ----------

circuits_df = circuits_df.select('circuitId',
 'circutName',
 'lat',
 'long',
 'locality',
 'country',
 'Timestamp',
 'filePath',
 'fileName')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rename Columns

# COMMAND ----------

circuits_df = (
    circuits_df.withColumnsRenamed(
        {
            "circuitId":"circuit_id",
            "circutName":"circuit_name",
            "lat":"latitude",
            "long":"longitude",
            "'filepath":"file_path",
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

circuits_df.filter(circuits_df.circuit_id.isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Drop Null values

# COMMAND ----------

Circuits_df = (
    circuits_df.filter(
        F.col("circuit_id").isNotNull()
    )
)


# COMMAND ----------

from pyspark.sql import functions as F

duplicate = circuits_df.groupBy(Circuits_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)



# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Duplicate values

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Use Drop Duplicates Method in circuit_id

# COMMAND ----------

Circuits_df = Circuits_df.dropDuplicates(['circuit_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Check the duplicate records has been dropped or not

# COMMAND ----------

from pyspark.sql import functions as F

duplicate = Circuits_df.groupBy(Circuits_df.columns)\
  .count()\
  .filter(F.col("count") > 1)
display(duplicate)

# COMMAND ----------

display(Circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Transform column values of circuit_name and locality to Title

# COMMAND ----------

# MAGIC %md
# MAGIC Here we need to change the columns value into title case, The function used for this scenarios are initcap Translate the first letter of each word to upper case in the sentence.

# COMMAND ----------

Circuits_cleaned_df = (
    Circuits_df
       .withColumn("circuit_name",F.initcap(F.col("circuit_name")))
       .withColumn("locality",F.initcap(F.col("locality")))

)

# COMMAND ----------

display(Circuits_cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write the cleaned and transformed data into silver table

# COMMAND ----------

silver_table

# COMMAND ----------

(
    Circuits_cleaned_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

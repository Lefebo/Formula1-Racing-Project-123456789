-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Project Enviroment Setup for Formula1 Racing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Project Requirements
-- MAGIC 1. Create External Location for the project
-- MAGIC 2. Create a catalog formula 1
-- MAGIC 3. Create Schemas landing, bronze, silver, gold
-- MAGIC 4. Create Volume Files in the landing schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Create External Location for the Project

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databricksstudy1234.dfs.core.windows.net/landing/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formula1
URL 'abfss://formula1@databricksstudy1234.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricksstudy123)
COMMENT 'External location for the formula Container'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Create a catalog formula 1

-- COMMAND ----------

show catalogs

-- COMMAND ----------

CREATE CATALOG if not exists Formula1
MANAGED LOCATION 'abfss://formula1@databricksstudy1234.dfs.core.windows.net/'

COMMENT "This is an external storage catalog for our formula 1 project"


-- COMMAND ----------

show catalogs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create Schema 

-- COMMAND ----------

CREATE SCHEMA if not exists formula1.landing;
CREATE SCHEMA if not exists formula1.bronze
MANAGED LOCATION "abfss://formula1@databricksstudy1234.dfs.core.windows.net/bronze";
CREATE SCHEMA if not exists formula1.silver
MANAGED LOCATION "abfss://formula1@databricksstudy1234.dfs.core.windows.net/silver";
CREATE SCHEMA if not exists formula1.gold
MANAGED LOCATION "abfss://formula1@databricksstudy1234.dfs.core.windows.net/gold";



-- COMMAND ----------

show schemas

-- COMMAND ----------

use catalog formula1;
show schemas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Create Volume Files in the landing schema

-- COMMAND ----------

Create external volume formula1.landing.files
Location 'abfss://formula1@databricksstudy1234.dfs.core.windows.net/landing';


-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

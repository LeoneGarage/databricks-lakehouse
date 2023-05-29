-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "", "Catalog to use")
-- MAGIC dbutils.widgets.text("schema", "", "Schema in Catalog to use")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE DATABASE {schema}")

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_date
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, could use a view when required. We ensure valid csv, date_id for demonstration purpose"
AS SELECT * 
FROM bronze_date
WHERE date_id IS NOT NULL AND _rescued_data IS NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_customer
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, customer_id for demonstration purpose"
AS SELECT * 
FROM bronze_customer
WHERE customer_id IS NOT NULL AND _rescued_data IS NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_product
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, product_id for demonstration purpose"
AS SELECT * 
FROM bronze_product
WHERE product_id IS NOT NULL AND _rescued_data IS NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_store
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, business_key for demonstration purpose"
AS SELECT * 
FROM bronze_store
WHERE business_key IS NOT NULL AND _rescued_data IS NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_sale
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, could use a view when required. We ensure valid csv, transaction_id, store for demonstration purpose"
AS SELECT * 
FROM bronze_sale
WHERE transaction_id IS NOT NULL AND store IS NOT NULL AND _rescued_data IS NULL

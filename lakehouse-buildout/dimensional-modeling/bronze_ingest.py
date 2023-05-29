# Databricks notebook source
dbutils.widgets.text("catalog", "", "Catalog to use")
dbutils.widgets.text("schema", "", "Schema in Catalog to use")
dbutils.widgets.text("raw-location", "", "Location of raw CSV files to ingest")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_location = dbutils.widgets.get("raw-location")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE DATABASE {schema}")

# COMMAND ----------

def bronze_date():
  (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{raw_location}/date/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_date")
  )

# COMMAND ----------

def bronze_customer():
 (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{raw_location}/customer/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_customer")
  )

# COMMAND ----------

def bronze_product():
  (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{raw_location}/product/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_product")
  )

# COMMAND ----------

def bronze_store():
  (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{raw_location}/store/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_store")
  )

# COMMAND ----------

def bronze_sale():
  (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{raw_location}/sale/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_sale")
  )

# COMMAND ----------

bronze_date()
bronze_customer()
bronze_product()
bronze_store()
bronze_sale()

# Databricks notebook source
import dlt

# COMMAND ----------

@dlt.table
def bronze_date():
  return (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{spark.conf.get('sales-pipeline.raw-location')}/date/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
  )

# COMMAND ----------

@dlt.table
def bronze_customer():
  return (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{spark.conf.get('sales-pipeline.raw-location')}/customer/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
  )

# COMMAND ----------

@dlt.table
def bronze_product():
  return (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{spark.conf.get('sales-pipeline.raw-location')}/product/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
  )

# COMMAND ----------

@dlt.table
def bronze_store():
  return (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{spark.conf.get('sales-pipeline.raw-location')}/store/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
  )

# COMMAND ----------

@dlt.table
def bronze_sale():
  return (
    spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{spark.conf.get('sales-pipeline.raw-location')}/sale/*.csv")
      .selectExpr("*", "_metadata.file_path as input_file_name")
  )

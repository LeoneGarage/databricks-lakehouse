-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC SELECT year(date)*10000 + month(date) * 100 + day(date) as date_id,
-- MAGIC  year(date)*10000 + month(date) * 100 + day(date) as date_num,
-- MAGIC  CAST(date AS STRING) as date,
-- MAGIC  year(date)*100 + month(date) as year_month_number,
-- MAGIC  CASE
-- MAGIC   WHEN month(date) = 2 OR month(date) = 3 OR month(date) = 4 THEN "Qtr 1"
-- MAGIC   WHEN month(date) = 5 OR month(date) = 6 OR month(date) = 7 THEN "Qtr 2"
-- MAGIC   WHEN month(date) = 8 OR month(date) = 9 OR month(date) = 10 THEN "Qtr 3"
-- MAGIC   WHEN month(date) = 11 OR month(date) = 12 OR month(date) = 1 THEN "Qtr 4"
-- MAGIC  END as calendar_quarter,
-- MAGIC  month(date) as month_num,
-- MAGIC  date_format(date, 'MMMM') as month_name
-- MAGIC FROM (
-- MAGIC select explode(sequence(to_date('2020-01-01'), to_date('2023-12-31'), interval 1 day)) as date
-- MAGIC )
-- MAGIC """).write.format("csv").option("header", True).save("/Users/leon.eller@databricks.com/sales/data/date1.csv")

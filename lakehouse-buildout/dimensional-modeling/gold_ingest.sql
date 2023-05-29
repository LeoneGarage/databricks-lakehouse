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

CREATE OR REPLACE TABLE dim_date
TBLPROPERTIES ("quality" = "gold")
COMMENT "Static Date dimension in the gold layer"
AS SELECT * 
FROM silver_date

-- COMMAND ----------

CREATE
OR REPLACE TABLE dim_customer(
  customer_id string,
  name string,
  email string,
  address string,
  created_date string,
  updated_date string
)

-- COMMAND ----------

MERGE INTO dim_customer USING (
  SELECT
    *
  EXCEPT
    (_rescued_data, input_file_name, _rn)
  FROM
    (
      SELECT
        *,
        row_number() OVER(
          PARTITION BY customer_id
          ORDER BY
            updated_date
        ) as _rn
      FROM
        silver_customer
    )
  WHERE
    _rn = 1
) as silver_customer ON dim_customer.customer_id = silver_customer.customer_id
WHEN MATCHED THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

CREATE
OR REPLACE TABLE dim_product (
  product_id INT,
  type STRING,
  SKU STRING,
  name STRING,
  published INT,
  is_featured INT,
  visibility_in_catalog STRING,
  short_description STRING,
  description STRING,
  weight_lbs DOUBLE,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  length_in INT,
  width_in DOUBLE,
  height_in DOUBLE,
  sale_price INT,
  regular_price DOUBLE,
  categories STRING
)

-- COMMAND ----------

MERGE INTO dim_product USING (
  SELECT
    *
  EXCEPT
    (_rescued_data, input_file_name, _rn)
  FROM
    (
      SELECT
        *,
        row_number() OVER(
          PARTITION BY product_id
          ORDER BY
            updated_date
        ) as _rn
      FROM
        silver_product
    )
  WHERE
    _rn = 1
) as silver_product ON dim_product.product_id = silver_product.product_id
WHEN MATCHED THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

CREATE
OR REPLACE TABLE dim_store (
  store_id INT,
  business_key STRING,
  name STRING,
  email STRING,
  city STRING,
  address STRING,
  phone_number STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)

-- COMMAND ----------

MERGE INTO dim_store USING (
  SELECT
    *
  EXCEPT
    (_rescued_data, input_file_name, _rn)
  FROM
    (
      SELECT
        *,
        row_number() OVER(
          PARTITION BY store_id
          ORDER BY
            updated_date
        ) as _rn
      FROM
        silver_store
    )
  WHERE
    _rn = 1
) as silver_store ON dim_store.store_id = silver_store.store_id
WHEN MATCHED THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_sale
TBLPROPERTIES ("quality" = "gold", "ignoreChanges" = "true")
COMMENT "sales fact table in the gold layer" AS
  SELECT
    sale.transaction_id,
    date.date_id,
    customer.customer_id,
    product.product_id as product_id,
    store.store_id,
    store.business_key as store_business_key,
    sales_amount
  from silver_sale sale
  inner join dim_date date
  on to_date(sale.transaction_date, 'M/d/yy') = to_date(date.date, 'M/d/yyyy') 
  -- only join with the active customers
  inner join (select * from dim_customer /*where __END_AT IS NULL*/) customer
  on sale.customer_id = customer.customer_id
  -- only join with the active products
  inner join (select * from dim_product /*where __END_AT IS NULL*/) product
  on sale.product = product.SKU
  -- only join with the active stores
  inner join (select * from dim_store /*where __END_AT IS NULL*/) store
  on sale.store = store.business_key
  WHERE store.business_key IS NOT NULL AND product_id IS NOT NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_daily_sale
TBLPROPERTIES ("quality" = "gold")
COMMENT "daily sales fact table in the gold layer" AS
  SELECT
    sale.date_id,
    sale.customer_id,
    sale.product_id,
    sale.store_id,
    sale.store_business_key,
    sum(sales_amount) as sales_amount
  from fact_sale sale
  WHERE date_id IS NOT NULL AND sales_amount IS NOT NULL
  group by 
    sale.date_id,
    sale.customer_id,
    sale.product_id,
    sale.store_id,
    sale.store_business_key

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_daily_store_sale
TBLPROPERTIES ("quality" = "gold")
COMMENT "daily sales fact table in the gold layer" AS
  SELECT
    sale.date_id,
    sale.store_id,
    sale.store_business_key,
    sum(sales_amount) as sales_amount
  from fact_sale sale
  WHERE date_id IS NOT NULL
  group by 
    sale.date_id,
    sale.store_id,
    sale.store_business_key

CREATE OR REPLACE TABLE `Datawarehouse_Banking.dim_customer` AS
SELECT DISTINCT
  customer_id,
  SAFE.PARSE_DATE('%Y-%m-%d', date) AS date,
  age,
  education,
  job,
  marital AS marital_status,
  loan AS loan_status,
  deposit,
  `default` AS default_status,
  balance
FROM `Dataavro.bank`;
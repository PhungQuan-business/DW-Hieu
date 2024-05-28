CREATE OR REPLACE TABLE `Datawarehouse_Banking.dim_date` AS
SELECT DISTINCT
  SAFE.PARSE_DATE('%Y-%m-%d', date) AS date,
  EXTRACT(DAY FROM SAFE.PARSE_DATE('%Y-%m-%d', date)) AS day,
  EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', date)) AS month,
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', date)) AS year,
  EXTRACT(QUARTER FROM SAFE.PARSE_DATE('%Y-%m-%d', date)) AS quarter
FROM `Dataavro.bank`
WHERE SAFE.PARSE_DATE('%Y-%m-%d', date) IS NOT NULL;
CREATE OR REPLACE TABLE `Datawarehouse_Banking.dim_campaign` AS
SELECT DISTINCT
  ROW_NUMBER() OVER() AS campaign_id,
  customer_id,
  campaign,
  pdays AS last_contacted_pcampaign,
  previous AS contacts_performed_before_campaign,
  poutcome
FROM `Dataavro.bank`;
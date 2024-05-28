CREATE OR REPLACE TABLE `Datawarehouse_Banking.fact_bank` AS
SELECT
    dc.campaign_id,
    dd.date,
    dc.last_contacted_pcampaign,
    dc.contacts_performed_before_campaign,
    dc.poutcome,
    dcus.age,
    dcus.education,
    dcus.job,
    dcus.marital_status,
    dcus.loan_status,
    dcus.deposit,
    dcus.default_status,
    dcus.balance
FROM
    `Datawarehouse_Banking.dim_campaign` dc
JOIN
    `Datawarehouse_Banking.dim_customer` dcus
ON
    dc.customer_id = dcus.customer_id
JOIN
    `Datawarehouse_Banking.dim_date` dd
ON
    dcus.date = dd.date;

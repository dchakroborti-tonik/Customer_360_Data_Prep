WITH params AS (
    -- Define the date parameters here
    SELECT 
        DATE('2024-01-01') AS start_date,
        DATE('2024-07-01') AS end_date
),
campaigns_base AS 
(
  ## customers who were part of any marketing campaign
  SELECT DISTINCT
  customer_id,
  event_time,
  event_code,
  event_name,
  a.campaign_name,
  a.campaign_channel,
  b.campaign_group
  FROM `moengage_raw.events_hourly` a
  JOIN `prj-prod-dataplatform.worktable_datachampions.20240829_moengage_marketing_campaigns_list` b ON a.campaign_name = b.campaign_name
  JOIN params p on date(event_time) BETWEEN p.start_date and p.end_date
  --JOIN `dl_customers_db_raw.tdbk_customer_mtb` c ON CAST(a.customer_id AS STRING) = c.cust_id 
)

#########################################
#### temp tables for the conversions ####
#########################################

### Transaction data
  ## Code is from 2024-08-28 Growth User Project - Transaction Features
  ## Used for FreeLuv and CreditBuilder conversions

, main_transaction_data AS 
(
    SELECT 
    transaction_date,
    c.created_dt registration_date,
    transaction_id,
    customer_id,
    account_type,
    a.transaction_code,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,

    -- customer_transactions to get the transactions
    FROM `risk_mart.customer_transactions` a
    
    -- customer_accounts to get when the deposit accounts were opened
    JOIN `finastra_raw.account` b ON a.customer_id = b.ubcustomercode AND a.accountid = b.accountid

    -- customer_mtb to get the date of onboarding
    JOIN `dl_customers_db_raw.tdbk_customer_mtb` c ON a.customer_id = c.cust_id
    JOIN params p on a.transaction_date BETWEEN p.start_date AND p.end_date

    -- customer_mtb to get the date of onboarding
    WHERE 1=1
    AND date(created_dt) = p.start_date
    and a.status = 'Success'
)

### Deposit Data
  ## Code is from 2024-08-28 Growth User Project - Deposit Account Details
  ## Used for credit builder campaign

, deposit_acc_main AS 
(
  SELECT DISTINCT
  a.opendate as ofdateopened,
  a.ubcustomercode as ofcustomerid,
  a.accountid as ofstandardaccountid,
  accountdescription as account_Type
  FROM finastra_raw.account  a
  WHERE 1=1
  AND accountdescription NOT IN ('Tonik Account')
)

### Cross-sell Offer Accepted 
  ## for Flex-Up and Reloan Offer 

, xsell_disbursed AS 
(
  SELECT DISTINCT
  customerid,
  disbursementdatetime,
  IF(reloan_flag = 1,"Reloan",new_loan_type) loan_type
  FROM `risk_credit_mis.loan_master_table`
  WHERE 1=1
  AND new_loan_type = 'Flex-up'
  AND flagDisbursement = 1
)

### Loan Applications
  ## for those who applied for QL/FL

, loan_apps AS 
(
  SELECT DISTINCT
  customerid,
  decision_date,
  new_loan_type
  FROM `risk_credit_mis.loan_master_table`
  WHERE 1=1
  AND new_loan_type IN ('Quick','Flex')
  AND decision_date IS NOT NULL
)

######################################
######################################
######################################

######################################
#### main query for the campaigns ####
######################################

### For the number of campaigns and campaign groups (Across all campaign groups)
, campaign_count AS 
(
SELECT DISTINCT
customer_id,

## Number of campaigns that a customer went through (Overall and Campaign Channel)
COUNT(DISTINCT(campaign_group)) no_of_campaign_groups_overall,
COUNT(DISTINCT(IF(campaign_channel = 'PUSH',campaign_group,NULL))) no_of_campaigns_push_overall,
COUNT(DISTINCT(IF(campaign_channel = 'EMAIL',campaign_group,NULL))) no_of_campaigns_email_overall,
COUNT(DISTINCT(IF(campaign_channel = 'SMS',campaign_group,NULL))) no_of_campaigns_sms_overall,
COUNT(DISTINCT(IF(campaign_channel = 'INAPP',campaign_group,NULL))) no_of_campaigns_inapp_overall,

## Number of times that the campaign was communicated to the customer (total number of comms)
COUNT(IF(
  event_name IN ('Email Delivered','Email Sent','Mobile In-App Shown','Notification Sent iOS','Notification Received Android'),
  customer_id,NULL)) no_times_communicated_overall,
COUNT(IF(campaign_channel = 'PUSH' AND 
  event_name IN ('Email Delivered','Email Sent','Mobile In-App Shown','Notification Sent iOS','Notification Received Android'),
  customer_id,NULL)) no_times_communicated_push_overall,
COUNT(IF(campaign_channel = 'EMAIL' AND 
  event_name IN ('Email Delivered','Email Sent','Mobile In-App Shown','Notification Sent iOS','Notification Received Android'),
  customer_id,NULL)) no_times_communicated_email_overall,
COUNT(IF(campaign_channel = 'SMS' AND 
  event_name IN ('Email Delivered','Email Sent','Mobile In-App Shown','Notification Sent iOS','Notification Received Android'),
  customer_id,NULL)) no_times_communicated_sms_overall,
COUNT(IF(campaign_channel = 'INAPP' AND 
  event_name IN ('Email Delivered','Email Sent','Mobile In-App Shown','Notification Sent iOS','Notification Received Android'),
  customer_id,NULL)) no_times_communicated_inapp_overall,
FROM campaigns_base
GROUP BY 1
)

## For the conversions (from Impression --> Click --> Conversion)
, impressions AS 
(
  ## Impressions
  SELECT DISTINCT customer_id, event_time, event_name, campaign_channel, campaign_name,campaign_group
  FROM campaigns_base
  WHERE 1=1
  AND event_name IN ('Email Opened','Mobile In-App Shown','Notification Received Android','Notification Sent iOS','SMS Sent')
)

, clicks AS
(
  ## Clicks
  SELECT DISTINCT customer_id, event_time, event_name, campaign_channel, campaign_name,campaign_group
  FROM campaigns_base
  WHERE 1=1
  AND event_name IN ('Mobile In-App Clicked','Notification Clicked Android','Notification Clicked iOS')
)

, impression_to_click as 
(
  ## impression to click to conversion
  SELECT DISTINCT
  a.customer_id,
  b.customer_id clicked_customer,
  a.event_time impression_time,
  b.event_time clicked_time,
  COALESCE(a.campaign_group,a.campaign_name) campaign,
  a.campaign_channel
  FROM impressions a
  LEFT JOIN clicks b
    ON a.customer_id = b.customer_id
    AND COALESCE(a.campaign_group,a.campaign_name) = COALESCE(b.campaign_group,b.campaign_name)
    AND a.campaign_channel = b.campaign_channel 
  WHERE 1=1
  AND (a.event_time <=  b.event_time OR b.event_time IS NULL)
)

#############################################################################
###### Split into the different campaigns for the Conversion Condition ######
#############################################################################

## 1. FreeLuv campaign conversions
    ## conversion condition: 2 bills pay, 2 virtual transactions, 2 top up minimum 50 each
, freeluv_conversions AS 
(
SELECT DISTINCT 
a.customer_id,
campaign,
CAST(IF(
COUNT(IF(credit_debit_indicator = 'DEBIT'AND LOWER(core_narration) NOT LIKE '%blocking%' AND channel = 'Billspay' AND trx_amount >= 50,transaction_id,NULL)) >= 2 AND 
COUNT(IF(credit_debit_indicator = 'DEBIT' AND account_type = 'Tonik Account' AND transaction_code like 'A0%' and core_narration not like '%Blocking%'AND trx_amount >= 50,transaction_id,NULL)) >= 2 AND
COUNT(IF(credit_debit_indicator = 'CREDIT' AND account_type = 'Tonik Account' and LOWER(core_narration) NOT LIKE '%blocking%' anD transaction_code not like 'A0%'
  AND transaction_code IN ('N01','IP2','XE2','00T','21C','P01')
  AND inter_exter_flag = 'Outside Tonik' AND trx_amount >= 50,transaction_id,NULL)) >= 2,
1,0) AS INT64) converted,
COUNT(impression_time) no_of_impressions_freeluv,
COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_freeluv,
COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_freeluv,
COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_freeluv,
COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_freeluv,
COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_freeluv,
COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_freeluv,
COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_freeluv,
COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_freeluv,
FROM impression_to_click a
LEFT JOIN main_transaction_data b 
      ON CAST(a.customer_id AS STRING) = b.customer_id
      ## used impression time as the date filter because there are some users who just saw the impression and did the action
      AND DATE(a.impression_time) <=  DATE(transaction_date)
WHERE 1=1
AND campaign = 'FreeLuv'
GROUP BY 1,2
)

## 2. Credit Builder campaign conversions
    ## conversion condition: bills pay, virtual transactions, top up, opened a deposit account (stash / time deposit)
, creditbuilder_conversions AS 
(
  SELECT DISTINCT 
  a.customer_id,
  campaign,
  CAST(IF(ofstandardaccountid IS NOT NULL OR transaction_id IS NOT NULL,1,0) AS INT64) converted,
  COUNT(impression_time) no_of_impressions_creditbuilder,
  COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_creditbuilder,
  COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_creditbuilder,
  COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_creditbuilder,
  COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_creditbuilder,
  COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_creditbuilder,
  COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_creditbuilder,
  COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_creditbuilder,
  COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_creditbuilder,
  FROM impression_to_click a
  LEFT JOIN main_transaction_data b 
        ON CAST(a.customer_id AS STRING) = b.customer_id
        AND DATE(a.impression_time) <=  DATE(transaction_date)
  LEFT JOIN deposit_acc_main c
        ON CAST(a.customer_id AS STRING) = c.ofcustomerid
        ## used impression time as the date filter because there are some users who just saw the impression and did the action
        AND DATE(a.impression_time) <= DATE(c.ofdateopened)
  WHERE 1=1
  AND campaign = 'CreditBuilder'
  GROUP BY 1,2,3
)

## 3. Push QL/FL
    ## conversion condition: bills pay, virtual transactions, top up, opened a deposit account (stash / time deposit)
, push_ql_fl_conversions AS 
(
  SELECT DISTINCT 
  a.customer_id,
  campaign,
  CAST(IF(b.customerid IS NOT NULL,1,0) AS INT64) converted,
  COUNT(impression_time) no_of_impressions_pushqlfl,
  COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_pushqlfl,
  COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_pushqlfl,
  COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_pushqlfl,
  COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_pushqlfl,
  COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_pushqlfl,
  COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_pushqlfl,
  COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_pushqlfl,
  COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_pushqlfl,
  FROM impression_to_click a
  LEFT JOIN loan_apps b
    ON CAST(a.customer_id AS STRING) = CAST(b.customerid AS STRING)
    ## used impression time as the date filter because there are some users who just saw the impression and did the action
    AND DATE(a.impression_time) <= DATE(decision_date)
  WHERE 1=1
  AND campaign = 'QL/FL Drop'
  GROUP BY 1,2,3
)

## 4. LuvStash
    ## conversion condition: open a stash
, luv_stash_conversions AS 
(
  SELECT DISTINCT 
  a.customer_id,
  campaign,
  CAST(IF(ofstandardaccountid IS NOT NULL,1,0) AS INT64) converted,
  COUNT(impression_time) no_of_impressions_luvstash,
  COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_luvstash,
  COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_luvstash,
  COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_luvstash,
  COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_luvstash,
  COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_luvstash,
  COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_luvstash,
  COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_luvstash,
  COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_luvstash,
  FROM impression_to_click a
  LEFT JOIN 
  (
    ## Isolate only for stash accounts
    SELECT DISTINCT *
    FROM deposit_acc_main b
    WHERE LOWER(account_Type) = '%stash%' 
  ) b
    ON CAST(a.customer_id AS STRING) = b.ofcustomerid
    ## used impression time as the date filter because there are some users who just saw the impression and did the action
    AND DATE(a.impression_time) <= DATE(ofdateopened)
  
  WHERE 1=1
  AND campaign = 'LuvStash Campaign'
  GROUP BY 1,2,3
)

## 5. Reloan-FlexUp Offer
  ## conversion condition: disbursed a reloan/flexup
, rlflup_offer_conversions AS 
(
  SELECT DISTINCT 
  a.customer_id,
  campaign,
  CAST(IF(b.customerid IS NOT NULL,1,0) AS INT64) converted,
  COUNT(impression_time) no_of_impressions_rlflup,
  COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_rlflup,
  COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_rlflup,
  COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_rlflup,
  COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_rlflup,
  COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_rlflup,
  COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_rlflup,
  COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_rlflup,
  COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_rlflup,
  FROM impression_to_click a
  LEFT JOIN xsell_disbursed b
    ON CAST(a.customer_id AS STRING) = CAST(b.customerid AS STRING)
    ## used impression time as the date filter because there are some users who just saw the impression and did the action
    AND DATE(a.impression_time) <= DATE(disbursementdatetime)
  WHERE 1=1
  AND campaign IN ('Flex Up and Reloan Offer')
  GROUP BY 1,2,3
)

## 6. These are campaigns that cannot have any conversion because it may just be for awareness or cannot be done
, other_campaigns AS 
(
  SELECT DISTINCT 
  a.customer_id,
  campaign,
  COUNT(impression_time) no_of_impressions_others,
  COUNT(IF(campaign_channel = 'PUSH',impression_time,NULL)) push_impressions_others,
  COUNT(IF(campaign_channel = 'EMAIL',impression_time,NULL)) email_impressions_others,
  COUNT(IF(campaign_channel = 'SMS',impression_time,NULL)) sms_impressions_others,
  COUNT(IF(campaign_channel = 'INAPP',impression_time,NULL)) inapp_impressions_others,
  COUNT(IF(campaign_channel = 'PUSH',clicked_time,NULL)) push_clicked_others,
  COUNT(IF(campaign_channel = 'EMAIL',clicked_time,NULL)) email_clicked_others,
  COUNT(IF(campaign_channel = 'SMS',clicked_time,NULL)) sms_clicked_others,
  COUNT(IF(campaign_channel = 'INAPP',clicked_time,NULL)) inapp_clicked_others,
  FROM impression_to_click a
  --LEFT JOIN transaction_conversion b 
  --      ON CAST(a.customer_id AS STRING) = b.customer_id
  --      AND DATE(a.impression_time) <=  DATE(transaction_date)
  LEFT JOIN deposit_acc_main c
        ON CAST(a.customer_id AS STRING) = c.ofcustomerid
        -- used impression time as the date filter because there are some users who just saw the impression and did the action
        AND DATE(a.impression_time) <= DATE(ofdateopened)
  WHERE 1=1

  ## remove the campaigns that we set the conversion condition to
  AND campaign NOT IN ('QL/FL Drop','FreeLuv','CreditBuilder','Flex Up and Reloan Offer','Luv_Back','LuvStash Campaign')
  GROUP BY 1,2
)


#########################################
############## Final Query ##############
#########################################

SELECT DISTINCT a.customer_id,
a.no_of_campaign_groups_overall,
a.no_of_campaigns_push_overall,
a.no_of_campaigns_email_overall,
a.no_of_campaigns_sms_overall,
a.no_of_campaigns_inapp_overall,
a.no_times_communicated_overall,
a.no_times_communicated_push_overall,
a.no_times_communicated_email_overall,
a.no_times_communicated_sms_overall,
a.no_times_communicated_inapp_overall,

## Number of Campaign Group that the user was converted to
b.converted + c.converted + d.converted + e.converted + f.converted total_campaign_group_conversion,

b.no_of_impressions_freeluv,
b.push_impressions_freeluv,
b.email_impressions_freeluv,
b.sms_impressions_freeluv,
b.inapp_impressions_freeluv,
b.push_clicked_freeluv,
b.email_clicked_freeluv,
b.sms_clicked_freeluv,
b.inapp_clicked_freeluv,
c.no_of_impressions_creditbuilder,
c.push_impressions_creditbuilder,
c.email_impressions_creditbuilder,
c.sms_impressions_creditbuilder,
c.inapp_impressions_creditbuilder,
c.push_clicked_creditbuilder,
c.email_clicked_creditbuilder,
c.sms_clicked_creditbuilder,
c.inapp_clicked_creditbuilder,

d.no_of_impressions_pushqlfl,
d.push_impressions_pushqlfl,
d.email_impressions_pushqlfl,
d.sms_impressions_pushqlfl,
d.inapp_impressions_pushqlfl,
d.push_clicked_pushqlfl,
d.email_clicked_pushqlfl,
d.sms_clicked_pushqlfl,
d.inapp_clicked_pushqlfl,

e.no_of_impressions_luvstash,
e.push_impressions_luvstash,
e.email_impressions_luvstash,
e.sms_impressions_luvstash,
e.inapp_impressions_luvstash,
e.push_clicked_luvstash,
e.email_clicked_luvstash,
e.sms_clicked_luvstash,
e.inapp_clicked_luvstash,

f.no_of_impressions_rlflup,
f.push_impressions_rlflup,
f.email_impressions_rlflup,
f.sms_impressions_rlflup,
f.inapp_impressions_rlflup,
f.push_clicked_rlflup,
f.email_clicked_rlflup,
f.sms_clicked_rlflup,
f.inapp_clicked_rlflup,

g.no_of_impressions_others,
g.push_impressions_others,
g.email_impressions_others,
g.sms_impressions_others,
g.inapp_impressions_others,
g.push_clicked_others,
g.email_clicked_others,
g.sms_clicked_others,
g.inapp_clicked_others,

FROM campaign_count a
LEFT JOIN freeluv_conversions b ON a.customer_id = b.customer_id
LEFT JOIN creditbuilder_conversions c ON a.customer_id = c.customer_id
LEFT JOIN push_ql_fl_conversions d ON a.customer_id = d.customer_id
LEFT JOIN luv_stash_conversions e ON a.customer_id = e.customer_id
LEFT JOIN rlflup_offer_conversions f ON a.customer_id = f.customer_id
LEFT JOIN other_campaigns g ON a.customer_id = g.customer_id

WITH params AS (
    -- Define the date parameters here
    SELECT 
        DATE('2024-01-01') AS start_date,
        DATE('2024-07-01') AS end_date
),
af_link AS
(
  ## To get the AF ID and Customer ID Link (using the first install of a customer)
  SELECT DISTINCT appsflyer_id, customer_user_id, install_time
  FROM `appsflyer_raw.in_app_events_report`
  JOIN params p on DATE(_partitiondate) BETWEEN p.start_date and p.end_date
  WHERE 1=1
  AND customer_user_id IS NOT NULL
  --AND DATE(_partitiondate) between '2023-11-01' and '2024-07-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_user_id ORDER BY install_time ASC) = 1
 
  UNION ALL
 
  SELECT DISTINCT appsflyer_id, customer_user_id, install_time
  FROM `appsflyer_raw.organic_in_app_events_report`
  JOIN params p on DATE(_partitiondate) BETWEEN p.start_date and p.end_date
  WHERE 1=1
  AND customer_user_id IS NOT NULL
  --AND DATE(_partitiondate) between '2023-11-01' and '2024-07-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_user_id ORDER BY install_time ASC) = 1
)
, events AS
(
  ## Gets all the instances of events for app launch, loan calculator, clicking of the QL and SIL Tiles
  SELECT DISTINCT
  event_name,
  event_Time,
  customer_id,
  --moengage_user_id,
  FROM `moengage_raw.events_hourly` a
  JOIN params p on DATE(event_time) BETWEEN p.start_date and p.end_date
  WHERE 1=1
  AND event_name IN ('App_Launch','Loans_QL_Calculator','Loans_QL_Launch','Loans_SIL_Launch')
 
  UNION ALL
 
  ## Triggers when clicking of the QL and SIL Tiles
  SELECT DISTINCT
  IF(clicked = 'Quick Loan','Loans_Selection - QL','Loans_Selection - SIL') event_name,
  event_Time,
  customer_id,
  --moengage_user_id,
  FROM `moengage_raw.events_hourly` a
  JOIN params p on DATE(event_time) BETWEEN p.start_date and p.end_date
  WHERE 1=1
  AND event_name IN ('Loans_Selection')
  AND clicked IN ('Quick_Loan','Shop_Installment_Loan')
  )
 
, f_cash_in AS 
(
  ## First Cash In of a user
  SELECT DISTINCT
  transactionid,
  narration,
  debitcreditflag,
  postingdate,
  amount,
  ubcustomercode,
  accountdescription,
  FROM finastra_raw.transactiondetails a
  JOIN finastra_raw.account b ON a.accountproduct_accprodid = b.accountid
  JOIN params p on DATE(postingdate) BETWEEN p.start_date and p.end_date
  --JOIN `dl_customers_db_raw.tdbk_customer_mtb` c ON b.ubcustomercode = c.cust_id
  WHERE 1=1
  AND debitcreditflag = 'C'
  AND code IN ('N01','IP2','XE2','00T','21C','P01') and ACCOUNTPRODUCT_ACCPRODID like '608%' and LOWER(narration) NOT LIKE '%blocking%'
  ## first cash-in in TSA
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ubcustomercode ORDER BY postingdate ASC) = 1
)
 
SELECT DISTINCT
cust_id,
appsflyer_id,
a.created_dt,
TIMESTAMP_DIFF(created_dt,install_time,MINUTE) install_to_registration_minutes,
TIMESTAMP_DIFF(postingdate,created_dt,MINUTE) onboarding_to_first_cash_in_minutes,
COUNT(IF(event_name = 'App Launch',customer_id,NULL)) app_launch_count_6mo,
COUNT(IF(
  event_name IN ('Loans_Selection - QL','Loans_QL_Launch'),customer_id,NULL)) ql_sales_tile_count_6mo,
COUNT(IF(
    event_name IN ('Loans_SIL_Launch','Loans_Selection - SIL'),customer_id,NULL)) sil_sales_tile_count_6mo,
COUNT(IF(
    event_name IN ('Loans_QL_Calculator'),customer_id,NULL)) ql_calculator_count_6mo
FROM `dl_customers_db_raw.tdbk_customer_mtb` a
JOIN af_link b ON a.cust_id = b.customer_user_id
JOIN params p on DATE(created_dt) = p.start_date
LEFT JOIN events c ON a.cust_id = CAST(c.customer_id AS STRING)
LEFT JOIN f_cash_in d ON a.cust_id = d.ubcustomercode
WHERE
TIMESTAMP_DIFF(created_dt,install_time,MINUTE) >= 0
group by 1,2,3,4,5
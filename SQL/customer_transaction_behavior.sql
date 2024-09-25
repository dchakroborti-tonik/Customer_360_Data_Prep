WITH params AS (
    -- Define the date parameters here
    SELECT 
        DATE('2024-01-01') AS start_date,
        DATE('2024-07-01') AS end_date
),

main_transaction_data AS 
(
     SELECT 
    transaction_date,
    opendate OFDATEOPENED,
    closed OFISCLOSED,
    c.created_dt registration_date,
    transaction_id,
    customer_id,
    a.accountid,
    productid,
    account_type,
    a.transaction_code,
    a.status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,

    -- customer_transactions to get the transactions
    FROM `dl_customers_db_raw.tdbk_customer_mtb` c  
    LEFT JOIN `risk_mart.customer_transactions` a ON a.customer_id = c.cust_id
    
    -- customer_accounts to get when the deposit accounts were opened
    JOIN `finastra_raw.account` b ON a.customer_id = b.ubcustomercode AND a.accountid = b.accountid
    JOIN params p on a.transaction_date BETWEEN p.start_date AND p.end_date

    -- customer_mtb to get the date of onboarding
    WHERE 1=1
    AND date(created_dt) = p.start_date
    and a.status = 'Success'
)

#### Net Cash In ####
  -- 1. Outside Tonik to TSA
  -- 2. Other Tonik user to Own Tonik Account


, net_cash_in AS 
(
  ## 1. Outside Tonik to TSA
  SELECT
    transaction_date,
    OFDATEOPENED,
    OFISCLOSED,
    registration_date,
    transaction_id,
    customer_id,
    accountid,
    account_type,
    status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,
    'Net Cash In' main_transaction_type,
    'Outside Tonik to TSA' sub_transaction_type,
  FROM main_transaction_data
  WHERE 1=1
  -- main conditions: should be a successful transaciton and credit and all coming from Tonik Account
  AND credit_debit_indicator = 'CREDIT'
  AND account_type = 'Tonik Account' and LOWER(core_narration) NOT LIKE '%blocking%' anD transaction_code not like 'A0%'
  AND transaction_code IN ('N01','IP2','XE2','00T','21C','P01')
  -- 1. Outside Tonik to TSA conditions (all cash in)
  AND inter_exter_flag = 'Outside Tonik'

  UNION ALL

  ## 2. Other Tonik user to Own Tonik Account
  SELECT
    transaction_date,
    OFDATEOPENED,
    OFISCLOSED,
    registration_date,
    transaction_id,
    customer_id,
    accountid,
    account_type,
    status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,
    'Net Cash In' main_transaction_type,
    'Other Tonik Users to Town Tonik Account' sub_transaction_type
  FROM main_transaction_data
  WHERE 1=1
  -- main conditions: should be a successful transaciton and credit and all coming from Tonik Account
  AND credit_debit_indicator = 'CREDIT'
  AND account_type = 'Tonik Account' and LOWER(core_narration) NOT LIKE '%blocking%' and transaction_code not like 'A0%'
  AND transaction_code IN ('N01','IP2','XE2','00T','21C','P01')

  -- 2. Other Tonik user to Own Tonik Account
  AND inter_exter_flag = 'Inside Tonik'
  AND core_narration LIKE '%Receive money from other Tonik Account%'
  -- AND LEFT(core_narration,STRPOS(core_narration, ",")-1) = 'Receive money from other Tonik Account'
)

#### Net Cash Out ####
-- 1. Bills Pay
-- 2. Card Transactions
-- 3. Own TSA to other Tonik Users
-- 4. TSA to Outside Tonik

, net_cash_out AS 
(

 ## 1. Bills Pay
  SELECT 
    transaction_date,
    OFDATEOPENED,
    OFISCLOSED,
    registration_date,
    transaction_id,
    customer_id,
    accountid,
    account_type,
    status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,
    'Net Cash Out' main_transaction_type,
    'Bills Pay' sub_transaction_type,
  FROM main_transaction_data
  WHERE 1=1
  -- main conditions: should be a successful transaciton and debit
  AND credit_debit_indicator = 'DEBIT'
  AND LOWER(core_narration) NOT LIKE '%blocking%'

  -- 1. Bills Pay
  AND channel = 'Billspay'


  UNION ALL

  ## 2. Card Transactions (Cash Out)
  SELECT
    a.transaction_date,
    a.OFDATEOPENED,
    a.OFISCLOSED,
    a.registration_date,
    a.transaction_id,
    a.customer_id,
    a.accountid,
    a.account_type,
    a.status,
    a.channel,
    a.credit_debit_indicator,
    a.inter_exter_flag,
    a.trx_amount,
    a.core_narration,
    'Net Cash Out' main_transaction_type,
    'Card Transactions (Cash Out)' sub_transaction_type
  FROM main_transaction_data a
  -- 2. Card Transactions (Cash Out) -- using the table made above
  WHERE 1=1
  -- main conditions: should be a successful transaciton and debit and coming from tonik account
  AND a.credit_debit_indicator = 'DEBIT'
  AND a.account_type = 'Tonik Account'
  AND transaction_code like 'A0%' and core_narration not like '%Blocking%'

  UNION ALL

  ## 3. Own TSA to other Tonik Users
  SELECT DISTINCT
    transaction_date,
    OFDATEOPENED,
    OFISCLOSED,
    registration_date,
    transaction_id,
    customer_id,
    accountid,
    account_type,
    status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,
    'Net Cash Out' main_transaction_type,
    'Own TSA to Other Tonik Users' sub_transaction_type
  FROM main_transaction_data a
  WHERE 1=1
  -- main conditions: should be a successful transaciton and debit
  AND a.credit_debit_indicator = 'DEBIT'
  AND a.account_type = 'Tonik Account'
  AND transaction_code not like 'A0%' and core_narration not like '%Blocking%'

  -- 3. Own TSA to other Tonik Users
  AND a.channel = 'Core transactions'
  AND a.inter_exter_flag = 'Inside Tonik'
  AND LOWER(core_narration) LIKE '%send money to other tonik account%' 
  -- AND LOWER(core_narration) NOT LIKE '%scontri%'
  -- AND LOWER(core_narration) NOT LIKE '%stash%'
  -- AND LOWER(core_narration) NOT LIKE '%time deposit%'

  UNION ALL

  ## 4. TSA to Outside Tonik (Other banks)
  SELECT DISTINCT
    transaction_date,
    OFDATEOPENED,
    OFISCLOSED,
    registration_date,
    transaction_id,
    customer_id,
    accountid,
    account_type,
    status,
    channel,
    credit_debit_indicator,
    inter_exter_flag,
    trx_amount,
    core_narration,
    'Net Cash Out' main_transaction_type,
    'TSA to Outside Tonik (Other Banks)' sub_transaction_type
  FROM main_transaction_data a
  WHERE 1=1
  -- main conditions: should be a successful transaciton and debit
  AND a.credit_debit_indicator = 'DEBIT'
  AND a.account_type = 'Tonik Account'
  AND core_narration not like '%Blocking%'

  -- channels not in core transactions and billspay with the flag as outside tonik are sending to other banks
  AND a.channel NOT IN  ('Core transactions','Billspay')
  AND a.inter_exter_flag = 'Outside Tonik'
)

, transactions_sub AS 
(
  -- merging the cash ins and cash outs
  SELECT DISTINCT *
  FROM net_cash_in 
  UNION ALL
  SELECT DISTINCT *
  FROM net_cash_out
)

, date_diff_sub AS 
(
    -- to get the date difference between 2 transactions (cash in and cash out)
    SELECT customer_id,
    'Overall' days_diff_type,
    DATE_DIFF(LEAD(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date ASC),transaction_date,DAY) days_bt_trans
    FROM 
    (
        SELECT DISTINCT
        transaction_date,
        customer_id,
        main_transaction_type,
        FROM transactions_sub
        WHERE 1=1
        --   AND customer_id IN ('2077378','2081999','2475220','2485072')
    )

    UNION ALL

    -- to get the date difference between 2 cash ins
    SELECT customer_id,
    'Cash In' days_diff_type,
    DATE_DIFF(LEAD(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date ASC),transaction_date,DAY) days_bt_trans
    FROM 
    (
        SELECT DISTINCT
        transaction_date,
        customer_id,
        main_transaction_type,
        FROM transactions_sub
        WHERE 1=1
        --   AND customer_id IN ('2077378','2081999','2475220','2485072')
        AND main_transaction_type = 'Net Cash In'
    )

    UNION ALL

    -- to get the date difference between 2 cash outs
    SELECT customer_id,
    'Cash Out' days_diff_type,
    DATE_DIFF(LEAD(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date ASC),transaction_date,DAY) days_bt_trans
    FROM 
    (
        SELECT DISTINCT
        transaction_date,
        customer_id,
        main_transaction_type,
        FROM transactions_sub
        WHERE 1=1
        --   AND customer_id IN ('2077378','2081999','2475220','2485072')
        AND main_transaction_type = 'Net Cash Out'
    )
)

, days_bt_trans_avg AS 
(
-- get the average days in between 
SELECT DISTINCT
customer_id,
AVG(IF(days_diff_type='Overall',days_bt_trans,NULL)) overall_avg_days_bt_trans,
AVG(IF(days_diff_type='Cash In',days_bt_trans,NULL)) net_cash_in_avg_days_bt_trans,
AVG(IF(days_diff_type='Cash Out',days_bt_trans,NULL)) net_cash_out_avg_days_bt_trans
FROM date_diff_sub
GROUP BY 1
)

, days_bt_trans_med AS 
(
-- get the median days in between
SELECT DISTINCT
customer_id,
PERCENTILE_CONT(IF(days_diff_type='Overall',days_bt_trans,NULL), .50) OVER (PARTITION BY customer_id) overall_med_days_bt_trans,
PERCENTILE_CONT(IF(days_diff_type='Cash In',days_bt_trans,NULL), .50) OVER (PARTITION BY customer_id) cash_in_med_days_bt_trans,
PERCENTILE_CONT(IF(days_diff_type='Cash Out',days_bt_trans,NULL), .50) OVER (PARTITION BY customer_id) cash_out_med_days_bt_trans,
FROM date_diff_sub
)


, transactions_final AS 
(

SELECT DISTINCT
customer_id,

## Number of transactions within the observation window (x days from onboarding date)
COUNT(DISTINCT transaction_id) total_transactions,

## Cash In Count Details
COUNT(DISTINCT(IF(main_transaction_type = 'Net Cash In',transaction_id,NULL))) cnt_cash_in_total,
COUNT(DISTINCT(IF(sub_transaction_type='Outside Tonik to TSA' ,transaction_id,NULL))) cnt_cash_in_ob2t,
COUNT(DISTINCT(IF(sub_transaction_type='Other Tonik Users to Town Tonik Account' ,transaction_id,NULL))) cnt_cash_in_ot2t,

## Cash In Amount Details
SUM(IF(main_transaction_type = 'Net Cash In',trx_amount,NULL)) amt_cash_in_total,
SUM((IF(sub_transaction_type='Outside Tonik to TSA' ,trx_amount,NULL))) amt_cash_in_ob2t,
SUM((IF(sub_transaction_type='Other Tonik Users to Town Tonik Account',trx_amount,NULL))) amt_cash_in_ot2t,

## Cash Out Count Details
COUNT(DISTINCT(IF(main_transaction_type = 'Net Cash Out',transaction_id,NULL))) cnt_cash_out_total,
COUNT(DISTINCT(IF(sub_transaction_type= 'Bills Pay' ,transaction_id,NULL))) cnt_cash_out_billpay,
COUNT(DISTINCT(IF(sub_transaction_type= 'Card Transactions (Cash Out)' ,transaction_id,NULL))) cnt_cash_out_cards,
COUNT(DISTINCT(IF(sub_transaction_type= 'Own TSA to Other Tonik Users' ,transaction_id,NULL))) cnt_cash_out_t2ot,
COUNT(DISTINCT(IF(sub_transaction_type= 'TSA to Outside Tonik (Other Banks)' ,transaction_id,NULL))) cnt_cash_out_t2ob,

## Cash Out Amount Details
SUM(IF(main_transaction_type = 'Net Cash Out',trx_amount,NULL)) amt_cash_out_total,
SUM(IF(sub_transaction_type= 'Bills Pay' ,trx_amount,NULL)) amt_cash_out_billpay,
SUM(IF(sub_transaction_type= 'Card Transactions (Cash Out)' ,trx_amount,NULL)) amt_cash_out_cards,
SUM(IF(sub_transaction_type= 'Own TSA to Other Tonik Users' ,trx_amount,NULL)) amt_cash_out_t2ot,
SUM(IF(sub_transaction_type= 'TSA to Outside Tonik (Other Banks)' ,trx_amount,NULL)) amt_cash_out_t2ob,
FROM transactions_sub a
GROUP BY 1
ORDER BY 2 
),
complete_deposit_metrics as (
  WITH deposit_acc_main AS 
(
  SELECT
  a.opendate as ofdateopened,
  a.ubcustomercode as customer_id,
  a.accountdescription as account_type,
  a.accountid as ofstandardaccountid,
  a.closuredate as ofdateclosed,
  balancedateasof,
  b.clearedbalance,
  a.closed,
  ff.status as td_status,
  FROM finastra_raw.account a
  LEFT JOIN `risk_mart.customer_balance` b on a.accountid = b.accountid
  LEFT JOIN `finastra_raw.fixturefeature` ff on ff.accountid = a.accountid
  JOIN params p on date(a.opendate) between p.start_date and p.end_date
  WHERE 1=1 
  and date(balancedateasof) = DATE_SUB(p.start_date,INTERVAL 1 DAY)
  and productid in ('savings','fixdep','SaveForFuture') and b.account_type <> 'Tonik Account'
)

,deposit_days_diff_sub as (
    SELECT customer_id,
    'Between All Deposits' days_diff_type,
    DATE_DIFF(LEAD(ofdateopened) OVER (PARTITION BY customer_id ORDER BY ofdateopened ASC),ofdateopened,DAY) days_bt_trans
    FROM deposit_acc_main

    UNION ALL

    SELECT customer_id,
    'Between TDs' days_diff_type,
    DATE_DIFF(LEAD(ofdateopened) OVER (PARTITION BY customer_id ORDER BY ofdateopened ASC),ofdateopened,DAY) days_bt_trans
    FROM deposit_acc_main
    WHERE account_type not like '%Stash%'

)
,dep_days_bt_trans_med AS 
(
-- get the median days in between
SELECT DISTINCT
customer_id,
PERCENTILE_CONT(IF(days_diff_type='Between All Deposits',days_bt_trans,NULL), .50) OVER (PARTITION BY customer_id) med_days_bw_new_dep_acct_open,
PERCENTILE_CONT(IF(days_diff_type='Between TDs',days_bt_trans,NULL), .50) OVER (PARTITION BY customer_id) med_days_bw_td_acct_open,
FROM deposit_days_diff_sub
)

, deposit_account_counts AS 
(
#### Number of Stash and Time Deposit accounts that are still open until the observation date with balance >= 100
SELECT DISTINCT
customer_id,
SUM(IF(account_type LIKE '%Stash%'AND clearedbalance>=100,clearedbalance,NULL)) stash_balance,
SUM(IF(account_type LIKE '%Time Deposit%' AND clearedbalance>=100,clearedbalance,NULL)) td_balance,
COUNT(DISTINCT(IF(account_type='Group Stash' AND clearedbalance>=100,ofstandardaccountid,NULL))) active_group_stash_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type='Individual Stash' AND clearedbalance>=100,ofstandardaccountid,NULL))) active_individual_stash_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Stash%' AND clearedbalance>=100,ofstandardaccountid,NULL))) active_stash_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Time Deposit%' AND clearedbalance>=100 and td_status not in ('4','9'),ofstandardaccountid,NULL))) active_td_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Time Deposit%' AND td_status = '4',ofstandardaccountid,NULL))) td_accounts_completed_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Time Deposit%' AND td_status = '9',ofstandardaccountid,NULL))) td_accounts_broken_cnt,
COUNT(DISTINCT ofstandardaccountid) deposit_accs_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Stash%',ofstandardaccountid,NULL))) stash_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Time Deposit%',ofstandardaccountid,NULL))) td_accounts_opened_cnt,
COUNT(DISTINCT(IF(account_type LIKE '%Stash%' and ofdateclosed between '2024-01-01' and '2024-07-01',ofstandardaccountid,NULL))) stash_accounts_closed_cnt,
FROM deposit_acc_main
GROUP BY 1
)

SELECT DISTINCT 
a.customer_id,
deposit_accs_cnt,
stash_accounts_opened_cnt,
stash_accounts_closed_cnt,
stash_balance,
td_accounts_opened_cnt,
td_accounts_completed_cnt,
td_accounts_broken_cnt,
td_balance,
med_days_bw_new_dep_acct_open,
med_days_bw_td_acct_open
FROM deposit_account_counts a
--LEFT JOIN deposit_account_90th_day_counts d ON a.customer_id = d.customer_id
LEFT JOIN dep_days_bt_trans_med b ON b.customer_id = a.customer_id
ORDER BY 2 DESC
),
loan_metrics as (
select customerId as customer_id,
count(DISTINCT(IF(applicationStatus = 'REJECT',digitalLoanAccountId,NULL))) num_rejected_loan_apps,
count(DISTINCT(IF(applicationStatus = 'COMPLETED',digitalLoanAccountId,NULL))) num_closed_loan_apps,
count(DISTINCT(IF(applicationStatus = 'ACTIVATED',digitalLoanAccountId,NULL))) num_active_loan_apps,
count(DISTINCT(IF(applicationStatus = 'APPLIED',digitalLoanAccountId,NULL))) num_applied_loan_apps,
count(DISTINCT(IF(applicationStatus = 'APPROVED',digitalLoanAccountId,NULL))) num_approved_loan_apps,
count(DISTINCT(IF(applicationStatus = 'ACCEPT',digitalLoanAccountId,NULL))) num_disbursed_loan_apps,
count(DISTINCT(IF(applicationStatus NOT IN ('ACCEPT','REJECT','COMPLETED','ACTIVATED','APPLIED','APPROVED'),digitalLoanAccountId,NULL))) num_other_loan_apps,
AVG(loanRequestTenure) avg_loan_request_tenure,
AVG(approvedLoanTenure) avg_loan_approved_tenure,
AVG(loanRequestAmount) avg_loan_request_amt,
AVG(approvedLoanAmount) avg_loan_approved_amt,
AVG(disbursedLoanAmount) avg_loan_disbursed_amt,
count(DISTINCT(IF(obsFPD10=1,digitalLoanAccountId,NULL))) as num_fpd_10,
count(DISTINCT(IF(obsFPD30=1,digitalLoanAccountId,NULL))) as num_fpd_30,
SUM(cast(numberofwholerepays as int64)) AS num_installments_paid,
SUM(repaymentpaid) as amt_installment_paid
FROM `risk_credit_mis.loan_master_table` lmt
LEFT JOIN (select numberofwholerepays,accountid from finastra_raw.loandetails ) loandetails on lmt.loanAccountNumber = loandetails.accountid
LEFT JOIN (select sum(repaymentpaid) as repaymentpaid,accountid from finastra_raw.loanrepayments group by 2 ) repayment on lmt.loanAccountNumber = repayment.accountid
JOIN params p on DATE(startApplyDateTime) between p.start_date and p.end_date
 group by 1
),

utility_transaction_data AS (
        SELECT 
        customer_id,
        CASE 
            WHEN SUM(CASE WHEN transaction_code IN ('BP1', 'BP2', 'BP3', 'BP4') THEN 1 ELSE 0 END) > 0 
                THEN MIN(transaction_date) 
            ELSE NULL 
        END AS first_billpay_date,
        CASE 
            WHEN SUM(CASE WHEN transaction_code like 'A0%' AND core_narration NOT LIKE '%Blocking%' THEN 1 ELSE 0 END) > 0 
                THEN MIN(transaction_date) 
            ELSE NULL 
        END AS virtual_transaction_date,
        CASE 
            WHEN SUM(CASE WHEN transaction_code IN ('21C', 'N01', 'IP2', 'XE2') THEN 1 ELSE 0 END) > 0 
                THEN MIN(transaction_date) 
            ELSE NULL 
        END AS first_tsa_topup_date
    FROM 
        main_transaction_data
    GROUP BY 1
),

combined_data AS (
    SELECT 
        COALESCE(acc_data.customer_id,utility_transaction_data.customer_id) customer_id,
        productid,
        accountdescription,
        acc_data.opendate as opendate ,
        first_billpay_date,
        virtual_transaction_date,
        first_tsa_topup_date,
        LEAST(
            IFNULL(DATE(acc_data.opendate),'9999-12-31'), 
            IFNULL(first_billpay_date, '9999-12-31'), 
            IFNULL(virtual_transaction_date, '9999-12-31'), 
            IFNULL(first_tsa_topup_date, '9999-12-31')
        ) AS first_opened_date
        FROM 
        utility_transaction_data
     LEFT JOIN 
        (SELECT customer_id,ofdateopened opendate ,productid,account_type as accountdescription from main_transaction_data 
        WHERE  account_type NOT IN ('Tonik Account')
       QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ofdateopened asc) = 1) acc_data
    ON 
        acc_data.customer_id = utility_transaction_data.customer_id
),
first_product_data as (
SELECT
    customer_id,
    --first_opened_date,
    CASE
        WHEN first_opened_date = DATE(opendate) THEN accountdescription
        WHEN first_opened_date = first_billpay_date THEN 'Bills Pay'
        WHEN first_opened_date = first_tsa_topup_date THEN 'TSA Top-Up'
        WHEN first_opened_date = virtual_transaction_date THEN 'Virtual Transaction'
        ELSE 'Unknown'
    END AS first_product,
    CASE
        WHEN first_opened_date = DATE(opendate) and productid in ('fixdep','savings','SaveForFuture') THEN 'Deposit Users'
        WHEN first_opened_date = DATE(opendate) and productid NOT IN ('fixdep','savings','SaveForFuture') THEN 'Loan Users'
        WHEN first_opened_date = first_tsa_topup_date or first_opened_date = virtual_transaction_date or first_opened_date = first_billpay_date THEN 'Utility Users'
        ELSE 'Ghost Users'
    END AS first_product_user_segment
FROM
    combined_data
)
SELECT
first_product_data.first_product,
first_product_data.first_product_user_segment,
a.*,
overall_avg_days_bt_trans avg_days_bt_trans,
net_cash_in_avg_days_bt_trans avg_days_bt_cash_in_trans,
net_cash_out_avg_days_bt_trans avg_days_bt_cash_out_trans,
overall_med_days_bt_trans med_days_bt_trans,
cash_in_med_days_bt_trans med_days_bt_cash_in_trans,
cash_out_med_days_bt_trans med_days_bt_cash_out_trans,
deposit_accs_cnt deposit_accnt_cnt,
stash_accounts_opened_cnt stash_accnt_opened_cnt,
stash_accounts_closed_cnt stash_accnt_closed_cnt,
stash_balance,
td_accounts_opened_cnt td_accnt_opened_cnt,
td_accounts_completed_cnt td_accnt_completed_cnt,
td_accounts_broken_cnt td_accnt_broken_cnt,
td_balance,
med_days_bw_new_dep_acct_open,
med_days_bw_td_acct_open,
num_rejected_loan_apps,
num_closed_loan_apps,
num_active_loan_apps,
num_applied_loan_apps,
num_approved_loan_apps,
num_disbursed_loan_apps,
num_other_loan_apps,
avg_loan_request_tenure,
avg_loan_approved_tenure,
avg_loan_request_amt,
avg_loan_approved_amt,
avg_loan_disbursed_amt,
num_fpd_10 num_fpd_10,
num_fpd_30 num_fpd_30,
num_installments_paid,
amt_installment_paid
FROM transactions_final a
JOIN days_bt_trans_avg b ON a.customer_id = b.customer_id
JOIN days_bt_trans_med c ON a.customer_id = c.customer_id
LEFT JOIN complete_deposit_metrics d on d.customer_id = a.customer_id
LEFT JOIN loan_metrics ON cast(loan_metrics.customer_id as string) = a.customer_id
LEFT JOIN first_product_data ON first_product_data.customer_id = a.customer_id

  create or replace   view Personal_Finance_Analytics_Pipeline.silver.int_transactions_enriched
  
  
  
  
  as (
    

select
    txn_id,
    txn_date,
    updated_at,
    txn_type,
    amount,
    category,
    subcategory,
    merchant,
    payment_mode,
    is_recurring,
    record_status,
    ingested_at,

    extract(year from txn_date) as txn_year,
    extract(month from txn_date) as txn_month,
    date_trunc('month', txn_date) as month_start_date,
    dayname(txn_date) as day_name,
    iff(dayofweekiso(txn_date) in (6, 7), true, false) as is_weekend,

    iff(txn_type = 'income', 1, 0) as is_income_flag,
    iff(txn_type = 'expense', 1, 0) as is_expense_flag,
    iff(txn_type = 'transfer', 1, 0) as is_transfer_flag,

    case
        when txn_type = 'income' then amount
        when txn_type = 'expense' then -amount
        when txn_type = 'transfer' then 0
        else 0
    end as net_amount

from Personal_Finance_Analytics_Pipeline.silver.stg_transactions
  );


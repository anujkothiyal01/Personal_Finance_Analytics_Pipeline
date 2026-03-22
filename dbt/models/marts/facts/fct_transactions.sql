{{ config(
    materialized='table',
    schema='gold'
) }}

select
    row_number() over (order by t.txn_id) as transaction_key,
    t.txn_id,
    to_number(to_char(t.txn_date, 'YYYYMMDD')) as date_key,
    dc.category_key,
    dm.merchant_key,
    dpm.payment_mode_key,

    t.txn_date,
    t.updated_at,
    t.txn_type,
    t.category,
    t.subcategory,
    t.merchant,
    t.payment_mode,
    t.amount,
    t.net_amount,
    t.is_recurring,
    t.record_status,
    t.ingested_at,
    t.txn_year,
    t.txn_month,
    t.month_start_date,
    t.day_name,
    t.is_weekend,
    t.is_income_flag,
    t.is_expense_flag,
    t.is_transfer_flag

from {{ ref('int_transactions_enriched') }} t
left join {{ ref('dim_category') }} dc
    on t.category = dc.category
   and t.subcategory = dc.subcategory
left join {{ ref('dim_merchant') }} dm
    on t.merchant = dm.merchant
left join {{ ref('dim_payment_mode') }} dpm
    on t.payment_mode = dpm.payment_mode
{{ config(
    materialized='table',
    schema='gold'
) }}

select
    month_start_date,
    txn_year,
    txn_month,

    sum(case when txn_type = 'income' then amount else 0 end) as total_income,
    sum(case when txn_type = 'expense' then amount else 0 end) as total_expense,
    sum(case when txn_type = 'transfer' then amount else 0 end) as total_transfer,

    sum(case when txn_type = 'income' then amount else 0 end)
      - sum(case when txn_type = 'expense' then amount else 0 end) as net_savings,

    round(
        (
            sum(case when txn_type = 'income' then amount else 0 end)
            - sum(case when txn_type = 'expense' then amount else 0 end)
        ) / nullif(sum(case when txn_type = 'income' then amount else 0 end), 0),
        4
    ) as savings_rate

from {{ ref('fct_transactions') }}
group by 1, 2, 3
order by month_start_date
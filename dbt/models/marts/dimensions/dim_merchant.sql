{{ config(
    materialized='table',
    schema='gold'
) }}

select
    row_number() over (order by merchant) as merchant_key,
    merchant
from (
    select distinct merchant
    from {{ ref('int_transactions_enriched') }}
)
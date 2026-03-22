{{ config(
    materialized='table',
    schema='gold'
) }}

select
    row_number() over (order by payment_mode) as payment_mode_key,
    payment_mode
from (
    select distinct payment_mode
    from {{ ref('int_transactions_enriched') }}
)
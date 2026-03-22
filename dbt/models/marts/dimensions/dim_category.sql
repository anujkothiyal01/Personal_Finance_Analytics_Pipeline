{{ config(
    materialized='table',
    schema='gold'
) }}

select
    row_number() over (order by category, subcategory) as category_key,
    category,
    subcategory
from (
    select distinct
        category,
        subcategory
    from {{ ref('int_transactions_enriched') }}
)
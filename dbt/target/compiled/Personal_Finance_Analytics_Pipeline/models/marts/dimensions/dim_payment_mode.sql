

select
    row_number() over (order by payment_mode) as payment_mode_key,
    payment_mode
from (
    select distinct payment_mode
    from Personal_Finance_Analytics_Pipeline.silver.int_transactions_enriched
)
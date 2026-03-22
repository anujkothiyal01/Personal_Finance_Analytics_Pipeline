
  
    

create or replace transient table Personal_Finance_Analytics_Pipeline.gold.dim_merchant
    
    
    
    as (

select
    row_number() over (order by merchant) as merchant_key,
    merchant
from (
    select distinct merchant
    from Personal_Finance_Analytics_Pipeline.silver.int_transactions_enriched
)
    )
;


  
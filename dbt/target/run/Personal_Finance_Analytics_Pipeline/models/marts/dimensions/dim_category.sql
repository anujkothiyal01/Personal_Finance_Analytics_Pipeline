
  
    

create or replace transient table Personal_Finance_Analytics_Pipeline.gold.dim_category
    
    
    
    as (

select
    row_number() over (order by category, subcategory) as category_key,
    category,
    subcategory
from (
    select distinct
        category,
        subcategory
    from Personal_Finance_Analytics_Pipeline.silver.int_transactions_enriched
)
    )
;


  
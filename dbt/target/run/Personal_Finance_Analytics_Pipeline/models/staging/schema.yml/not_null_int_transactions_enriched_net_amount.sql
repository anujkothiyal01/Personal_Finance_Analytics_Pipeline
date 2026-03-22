
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_amount
from Personal_Finance_Analytics_Pipeline.silver.int_transactions_enriched
where net_amount is null



  
  
      
    ) dbt_internal_test
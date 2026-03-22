
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from Personal_Finance_Analytics_Pipeline.silver.stg_transactions
where amount is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_key
from Personal_Finance_Analytics_Pipeline.gold.fct_transactions
where transaction_key is null



  
  
      
    ) dbt_internal_test
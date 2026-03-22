
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_key
from Personal_Finance_Analytics_Pipeline.gold.fct_transactions
where date_key is null



  
  
      
    ) dbt_internal_test
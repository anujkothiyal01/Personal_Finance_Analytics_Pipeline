
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_mode_key
from Personal_Finance_Analytics_Pipeline.gold.dim_payment_mode
where payment_mode_key is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select merchant_key
from Personal_Finance_Analytics_Pipeline.gold.dim_merchant
where merchant_key is null



  
  
      
    ) dbt_internal_test
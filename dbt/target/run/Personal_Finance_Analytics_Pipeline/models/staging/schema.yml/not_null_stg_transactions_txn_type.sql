
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select txn_type
from Personal_Finance_Analytics_Pipeline.silver.stg_transactions
where txn_type is null



  
  
      
    ) dbt_internal_test
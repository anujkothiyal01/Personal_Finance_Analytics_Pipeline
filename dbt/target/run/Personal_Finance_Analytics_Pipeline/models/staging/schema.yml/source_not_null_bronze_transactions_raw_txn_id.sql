
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select txn_id
from Personal_Finance_Analytics_Pipeline.bronze.transactions_raw
where txn_id is null



  
  
      
    ) dbt_internal_test
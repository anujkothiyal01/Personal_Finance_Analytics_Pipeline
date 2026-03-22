
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select txn_date
from Personal_Finance_Analytics_Pipeline.bronze.transactions_raw
where txn_date is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    transaction_key as unique_field,
    count(*) as n_records

from Personal_Finance_Analytics_Pipeline.gold.fct_transactions
where transaction_key is not null
group by transaction_key
having count(*) > 1



  
  
      
    ) dbt_internal_test
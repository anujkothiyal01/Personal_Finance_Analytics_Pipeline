
  
    

create or replace transient table Personal_Finance_Analytics_Pipeline.gold.mart_category_spend
    
    
    
    as (

select
    month_start_date,
    category,
    subcategory,
    sum(case when txn_type = 'expense' then amount else 0 end) as total_expense,
    count(*) as transaction_count
from Personal_Finance_Analytics_Pipeline.gold.fct_transactions
group by 1, 2, 3
order by month_start_date, category, subcategory
    )
;


  
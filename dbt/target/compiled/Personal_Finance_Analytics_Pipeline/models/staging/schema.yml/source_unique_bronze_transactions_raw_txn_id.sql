
    
    

select
    txn_id as unique_field,
    count(*) as n_records

from Personal_Finance_Analytics_Pipeline.bronze.transactions_raw
where txn_id is not null
group by txn_id
having count(*) > 1




    
    

select
    merchant_key as unique_field,
    count(*) as n_records

from Personal_Finance_Analytics_Pipeline.gold.dim_merchant
where merchant_key is not null
group by merchant_key
having count(*) > 1



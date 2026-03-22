
    
    

select
    payment_mode_key as unique_field,
    count(*) as n_records

from Personal_Finance_Analytics_Pipeline.gold.dim_payment_mode
where payment_mode_key is not null
group by payment_mode_key
having count(*) > 1




    
    

with all_values as (

    select
        txn_type as value_field,
        count(*) as n_records

    from Personal_Finance_Analytics_Pipeline.silver.stg_transactions
    group by txn_type

)

select *
from all_values
where value_field not in (
    'income','expense','transfer'
)



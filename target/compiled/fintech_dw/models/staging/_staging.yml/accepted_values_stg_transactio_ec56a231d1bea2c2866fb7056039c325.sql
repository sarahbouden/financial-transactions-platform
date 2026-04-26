
    
    

with all_values as (

    select
        transaction_type as value_field,
        count(*) as n_records

    from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
    group by transaction_type

)

select *
from all_values
where value_field not in (
    'purchase','refund','transfer','withdrawal','deposit'
)



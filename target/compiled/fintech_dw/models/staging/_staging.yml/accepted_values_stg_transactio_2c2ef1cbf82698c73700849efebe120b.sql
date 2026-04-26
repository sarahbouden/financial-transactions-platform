
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
    group by status

)

select *
from all_values
where value_field not in (
    'completed','pending','failed','reversed'
)



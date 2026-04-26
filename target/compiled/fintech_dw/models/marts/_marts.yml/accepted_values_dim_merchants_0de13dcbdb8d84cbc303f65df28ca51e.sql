
    
    

with all_values as (

    select
        risk_segment as value_field,
        count(*) as n_records

    from `crucial-module-493808-q9`.`fintech_dw_marts`.`dim_merchants`
    group by risk_segment

)

select *
from all_values
where value_field not in (
    'low_risk','medium_risk','high_risk'
)



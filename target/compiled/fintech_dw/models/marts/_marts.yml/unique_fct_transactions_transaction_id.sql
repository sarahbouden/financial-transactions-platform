
    
    

with dbt_test__target as (

  select transaction_id as unique_field
  from `crucial-module-493808-q9`.`fintech_dw_marts`.`fct_transactions`
  where transaction_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



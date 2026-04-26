
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        currency as value_field,
        count(*) as n_records

    from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
    group by currency

)

select *
from all_values
where value_field not in (
    'EUR','USD','GBP','CHF'
)



  
  
      
    ) dbt_internal_test
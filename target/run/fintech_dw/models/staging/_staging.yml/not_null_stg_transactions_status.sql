
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
where status is null



  
  
      
    ) dbt_internal_test
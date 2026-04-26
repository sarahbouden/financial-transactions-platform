
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_timestamp
from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
where transaction_timestamp is null



  
  
      
    ) dbt_internal_test
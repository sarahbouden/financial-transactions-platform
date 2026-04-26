
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_type
from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
where transaction_type is null



  
  
      
    ) dbt_internal_test
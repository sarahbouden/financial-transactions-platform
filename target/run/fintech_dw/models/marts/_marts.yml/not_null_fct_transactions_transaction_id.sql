
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_id
from `crucial-module-493808-q9`.`fintech_dw_marts`.`fct_transactions`
where transaction_id is null



  
  
      
    ) dbt_internal_test
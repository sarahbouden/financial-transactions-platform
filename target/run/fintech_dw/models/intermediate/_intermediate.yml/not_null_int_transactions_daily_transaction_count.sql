
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_count
from `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_transactions_daily`
where transaction_count is null



  
  
      
    ) dbt_internal_test
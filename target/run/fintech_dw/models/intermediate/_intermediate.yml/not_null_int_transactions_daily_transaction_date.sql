
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_date
from `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_transactions_daily`
where transaction_date is null



  
  
      
    ) dbt_internal_test
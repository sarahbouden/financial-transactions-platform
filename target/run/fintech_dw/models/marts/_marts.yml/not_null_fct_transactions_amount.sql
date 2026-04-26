
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from `crucial-module-493808-q9`.`fintech_dw_marts`.`fct_transactions`
where amount is null



  
  
      
    ) dbt_internal_test
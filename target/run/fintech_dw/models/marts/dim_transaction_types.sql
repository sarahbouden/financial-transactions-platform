
  
    

    create or replace table `crucial-module-493808-q9`.`fintech_dw_marts`.`dim_transaction_types`
      
    
    

    
    OPTIONS()
    as (
      with daily as (
    select * from `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_transactions_daily`
)

select
    transaction_type,
    currency,
    sum(transaction_count)                  as total_count,
    sum(total_amount)                       as total_amount,
    avg(avg_amount)                         as overall_avg_amount,
    sum(fraud_count)                        as total_fraud_count,
    safe_divide(
        sum(fraud_count),
        sum(transaction_count)
    )                                       as overall_fraud_rate
from daily
group by 1, 2
    );
  

  
    

    create or replace table `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_transactions_daily`
      
    
    

    
    OPTIONS()
    as (
      with stg as (
    select * from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
)

select
    date(transaction_timestamp)             as transaction_date,
    transaction_type,
    status,
    currency,
    count(*)                                as transaction_count,
    sum(amount)                             as total_amount,
    avg(amount)                             as avg_amount,
    min(amount)                             as min_amount,
    max(amount)                             as max_amount,
    countif(is_fraud = true)                as fraud_count,
    safe_divide(
        countif(is_fraud = true),
        count(*)
    )                                       as fraud_rate
from stg
group by 1, 2, 3, 4
    );
  
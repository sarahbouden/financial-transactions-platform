
  
    

    create or replace table `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_merchant_stats`
      
    
    

    
    OPTIONS()
    as (
      with stg as (
    select * from `crucial-module-493808-q9`.`fintech_dw_staging`.`stg_transactions`
)

select
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    count(*)                                as total_transactions,
    sum(case when status = 'completed'
        then amount else 0 end)             as total_revenue,
    avg(amount)                             as avg_transaction_amount,
    countif(is_fraud = true)                as fraud_count,
    safe_divide(
        countif(is_fraud = true),
        count(*)
    )                                       as fraud_rate,
    min(transaction_timestamp)              as first_seen_at,
    max(transaction_timestamp)              as last_seen_at
from stg
group by 1, 2, 3, 4
    );
  
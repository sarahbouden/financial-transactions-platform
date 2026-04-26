
  
    

    create or replace table `crucial-module-493808-q9`.`fintech_dw_marts`.`dim_merchants`
      
    
    

    
    OPTIONS()
    as (
      with merchant_stats as (
    select * from `crucial-module-493808-q9`.`fintech_dw_intermediate`.`int_merchant_stats`
)

select
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    total_transactions,
    total_revenue,
    avg_transaction_amount,
    fraud_count,
    fraud_rate,
    first_seen_at,
    last_seen_at,
    -- Segment risque simple basé sur le taux de fraude
    case
        when fraud_rate >= 0.05 then 'high_risk'
        when fraud_rate >= 0.02 then 'medium_risk'
        else 'low_risk'
    end                                     as risk_segment
from merchant_stats
    );
  
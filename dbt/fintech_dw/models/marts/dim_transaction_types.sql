with daily as (
    select * from {{ ref('int_transactions_daily') }}
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
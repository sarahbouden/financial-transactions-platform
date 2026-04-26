with stg as (
    select * from {{ ref('stg_transactions') }}
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
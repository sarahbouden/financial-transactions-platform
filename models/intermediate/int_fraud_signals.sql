with stg as (
    select * from {{ ref('stg_transactions') }}
)

select
    merchant_category,
    card_type,
    device_type,
    count(*)                                as total_transactions,
    countif(is_fraud = true)                as fraud_count,
    safe_divide(
        countif(is_fraud = true),
        count(*)
    )                                       as fraud_rate,
    avg(case when is_fraud then amount end) as avg_fraud_amount
from stg
group by 1, 2, 3
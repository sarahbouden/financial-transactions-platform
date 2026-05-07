with stg as (
    select * from {{ ref('stg_transactions') }}
),

merchant_stats as (
    select
        merchant_id,
        fraud_rate              as merchant_fraud_rate,
        total_transactions      as merchant_total_transactions
    from {{ ref('int_merchant_stats') }}
)

select
    -- Clés
    stg.transaction_id,
    stg.merchant_id,
    stg.customer_id,

    -- Montant
    stg.amount,
    stg.currency,

    -- Dimensions
    stg.transaction_type,
    stg.status,
    stg.card_type,
    stg.device_type,

    -- Fraude
    stg.is_fraud,
    ms.merchant_fraud_rate,

    -- Dates (pour faciliter les filtres analytiques)
    stg.transaction_timestamp,
    date(stg.transaction_timestamp)         as transaction_date,
    extract(hour from stg.transaction_timestamp) as transaction_hour,

    -- Metadata pipeline
    stg.ingested_at,
    timestamp_diff(
        stg.ingested_at,
        stg.transaction_timestamp,
        second
    )                                       as pipeline_latency_seconds

from stg
left join merchant_stats ms
    on stg.merchant_id = ms.merchant_id
-- ci verification

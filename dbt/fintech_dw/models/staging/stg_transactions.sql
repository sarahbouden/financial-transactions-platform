with source as (
    select
        data,
        publish_time,
        message_id
    from {{ source('raw', 'raw_transactions') }}
    where data is not null
),

parsed as (
    select
        -- Identifiants
        json_value(data, '$.transaction_id')        as transaction_id,
        json_value(data, '$.merchant_id')           as merchant_id,
        json_value(data, '$.customer_id')           as customer_id,

        -- Montant et devise
        cast(json_value(data, '$.amount') as float64)  as amount,
        json_value(data, '$.currency')              as currency,

        -- Type et statut
        json_value(data, '$.transaction_type')      as transaction_type,
        json_value(data, '$.status')                as status,

        -- Marchand
        json_value(data, '$.merchant_name')         as merchant_name,
        json_value(data, '$.merchant_category')     as merchant_category,
        json_value(data, '$.merchant_country')      as merchant_country,

        -- Client
        json_value(data, '$.customer_email')        as customer_email,

        -- Carte
        json_value(data, '$.card_type')             as card_type,
        json_value(data, '$.card_last_four')        as card_last_four,

        -- Device
        json_value(data, '$.device_type')           as device_type,
        json_value(data, '$.ip_address')            as ip_address,

        -- Fraude
        cast(json_value(data, '$.is_fraud') as bool) as is_fraud,

        -- Timestamps
        cast(
            json_value(data, '$.timestamp') as timestamp
        )                                           as transaction_timestamp,
        publish_time                                as ingested_at,
        message_id                                  as pubsub_message_id

    from source
),

-- Déduplication : garde la première occurrence par transaction_id
deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by ingested_at asc
        ) as row_num
    from parsed
    where transaction_id is not null
)

select
    transaction_id,
    merchant_id,
    customer_id,
    amount,
    currency,
    transaction_type,
    status,
    merchant_name,
    merchant_category,
    merchant_country,
    customer_email,
    card_type,
    card_last_four,
    device_type,
    ip_address,
    is_fraud,
    transaction_timestamp,
    ingested_at,
    pubsub_message_id
from deduplicated
where row_num = 1
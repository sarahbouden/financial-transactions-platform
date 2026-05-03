-- Test custom : vérifie que les purchases et deposits sont toujours positifs
select
    transaction_id,
    amount,
    transaction_type
from {{ ref('fct_transactions') }}
where transaction_type in ('purchase', 'deposit')
  and amount <= 0
-- Materialized view: raw balances of every CoW transaction in the last 10 days
-- Read by the imbalance query instead of solana.transactions.

with cow_txs as ( -- every tx with a CoW settlement / create_buffer / reclaim / native SOL withdrawal
    select call_tx_id as tx_id, call_block_slot as block_slot, call_block_date as block_date
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where call_block_date >= current_date - interval '10' day
    union distinct
    select call_tx_id, call_block_slot, call_block_date
    from cow_protocol_solana.cow_settlement_call_create_buffer
    where call_block_date >= current_date - interval '10' day
    union distinct
    select call_tx_id, call_block_slot, call_block_date
    from cow_protocol_solana.cow_settlement_call_reclaim_buffer
    where call_block_date >= current_date - interval '10' day
    union distinct
    select call_tx_id, call_block_slot, call_block_date
    from cow_protocol_solana.cow_settlement_call_withdraw_native_sol
    where call_block_date >= current_date - interval '10' day
)

select
    t.id as tx_id,
    t.block_time,
    t.block_slot,
    t.block_date,
    t.signer,
    t.pre_token_balances,
    t.post_token_balances,
    t.pre_balances,
    t.post_balances,
    t.account_keys,
    t.loaded_addresses
from solana.transactions as t
inner join cow_txs as c
    on c.tx_id = t.id
    and c.block_slot = t.block_slot
    and c.block_date = t.block_date
where
    t.block_date >= current_date - interval '10' day
    and t.success

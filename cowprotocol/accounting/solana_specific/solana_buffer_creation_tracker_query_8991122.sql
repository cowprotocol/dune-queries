-- Query to track all rent deposits per created shared buffer (lamports)
-- together with the solver that paid this rent.

with created as ( -- one row per (buffer, mint) created
    select
        cb.call_tx_id as tx_id,
        cb.call_block_time as block_time,
        cb.call_block_slot as block_slot,
        cb.account_payer as payer,
        t.account as buffer_account,
        cb.call_account_arguments[t.pos + 1] as mint
    from cow_protocol_solana.cow_settlement_call_create_buffer as cb
    -- args: [payer, system_program, token_program, (buffer, mint)...]
    cross join unnest(cb.call_account_arguments) with ordinality as t(account, pos)
    where
        t.pos >= 4 and t.pos % 2 = 0
        and cb.call_block_time >= cast('{{start_time}}' as timestamp)
        and cb.call_block_time <= cast('{{end_time}}' as timestamp)
),

settle_txs as ( -- to flag buffers created inside a settlement tx
    select distinct call_tx_id as tx_id
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
),

buffer_lamports as ( -- buffer's lamport balance before/after its creating tx
    select
        c.*,
        element_at(tx.pre_balances,  nullif(array_position(tx.account_keys, c.buffer_account), 0)) as pre_lamports,
        element_at(tx.post_balances, nullif(array_position(tx.account_keys, c.buffer_account), 0)) as post_lamports
    from created as c inner join solana.transactions as tx
        on tx.id = c.tx_id and tx.block_time = c.block_time
    where
        tx.block_time >= cast('{{start_time}}' as timestamp)
        and tx.block_time <= cast('{{end_time}}' as timestamp)
        and tx.success
)

select
    b.tx_id,
    b.block_time,
    b.block_slot,
    b.payer,
    b.buffer_account,
    b.mint,
    s.tx_id is not null as in_settlement_tx,
    b.pre_lamports,
    b.post_lamports,
    b.post_lamports - b.pre_lamports as rent_lamports
from buffer_lamports as b left join settle_txs s on s.tx_id = b.tx_id
order by b.block_time desc

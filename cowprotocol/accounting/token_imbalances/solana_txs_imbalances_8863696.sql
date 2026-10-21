-- This a base query for computing token imbalances (after - before)
-- of the CoW Protocol contract on Solana per tx id.
-- Note it doesn't track imbalances resulting from non-settlements.
-- Built on decoded tables: cow_protocol_solana.cow_settlement_call_*

with buffers as ( -- args: [payer, system_program, token_program, (buffer, mint)...]
    select distinct t.account as buffer_account
    from cow_protocol_solana.cow_settlement_call_create_buffer as cb
    cross join unnest(cb.call_account_arguments) with ordinality as t(account, pos)
    where pos >= 4 and pos % 2 = 0
),

settle_txs as ( -- settlement txs in window
    select distinct
        call_tx_id as tx_id,
        call_block_time as block_time,
        auction_id
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
),

settlements as (
    select
        t.id as tx_id,
        t.block_time,
        s.auction_id,
        t.pre_token_balances,
        t.post_token_balances,
        t.account_keys,
        t.pre_balances,
        t.post_balances
    from solana.transactions as t inner join settle_txs s
        on s.tx_id = t.id and s.block_time = t.block_time
    where
        t.block_time >= cast('{{start_time}}' as timestamp)
        and t.block_time <= cast('{{end_time}}' as timestamp)
        and t.success
),

pre as (
    select
        s.tx_id,
        s.block_time,
        s.auction_id,
        b.account,
        b.mint,
        b.amount
    from settlements as s
    cross join unnest(s.pre_token_balances) as b(account, mint, owner, amount)
    inner join buffers on buffers.buffer_account = b.account
),

post as (
    select
        s.tx_id,
        s.block_time,
        s.auction_id,
        b.account,
        b.mint,
        b.amount
    from settlements as s
    cross join unnest(s.post_token_balances) as b(account, mint, owner, amount)
    inner join buffers on buffers.buffer_account = b.account
),

token_units as (
    select
        coalesce(post.tx_id, pre.tx_id) as tx_id,
        coalesce(post.block_time, pre.block_time) as block_time,
        coalesce(post.auction_id, pre.auction_id) as auction_id,
        coalesce(post.account, pre.account) as buffer_account,
        coalesce(post.mint, pre.mint) as mint,
        coalesce(pre.amount, 0) as pre_amount,
        coalesce(post.amount, 0) as post_amount
    from pre full outer join post
        on pre.tx_id = post.tx_id and pre.account = post.account
),

token_diffs as ( -- scale back to atoms (exact: DECIMAL math only)
    select
        u.tx_id,
        u.block_time,
        u.auction_id,
        u.buffer_account,
        u.mint,
        cast(u.pre_amount  * cast(power(10, f.decimals) as decimal(20,0)) as decimal(38,0)) as pre_atoms,
        cast(u.post_amount * cast(power(10, f.decimals) as decimal(20,0)) as decimal(38,0)) as post_atoms
    from token_units as u left join tokens_solana.fungible as f on f.token_mint_address = u.mint
),

native_diffs as ( -- native SOL buffer: lamports are already atoms
    select
        s.tx_id,
        s.block_time,
        s.auction_id,
        'ESnpC8iNkuUzbLRm2dG6VezaGBi86QPdYh6TzvSXn2Pz' as buffer_account,
        '11111111111111111111111111111111' as mint,
        cast(s.pre_balances[p] as decimal(38,0)) as pre_atoms,
        cast(s.post_balances[p] as decimal(38,0)) as post_atoms
    from settlements as s
    cross join unnest(array[array_position(s.account_keys, 'ESnpC8iNkuUzbLRm2dG6VezaGBi86QPdYh6TzvSXn2Pz')]) as x(p)
    where p > 0
)

select
    tx_id,
    block_time,
    auction_id,
    buffer_account,
    mint as token_address,
    pre_atoms,
    post_atoms,
    post_atoms - pre_atoms as amount
from (
    select * from token_diffs
    union all
    select * from native_diffs
)
where post_atoms is distinct from pre_atoms
order by block_time desc

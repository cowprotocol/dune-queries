-- CoW settlement (prod): buffer imbalances per tx, with solver attribution.
--
-- Solana analogue of the EVM token-imbalance queries (4021257 / 4021644). Unlike the EVM
-- version it does NOT look at every transfer touching the buffers: it only scans txs that
-- contain a CoW instruction (begin_settle / create_buffer / reclaim_buffer / withdraw_native_sol).
-- Buffer in-flows made in other txs (e.g. a solver pre-funding a buffer) are not visible here.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}}   - the timestamp for which the analysis should end (exclusively)
--
-- Output: one row per (tx, buffer, leg, token) with a non-zero (or NULL) balance change, where
--  amount      : post - pre, in atoms (NULL when the mint is missing from tokens_solana.fungible)
--  leg         : token      - token balance change of a buffer
--                native_sol - lamport change of the native SOL buffer (its balance IS the SOL buffer)
--                lamports   - lamport change of a token buffer, i.e. rent on creation / reclaim,
--                             net of wSOL token movements on the wSOL buffer (no double count)
--  attribution : settlement / multi_solver / protocol / solver_non_settlement / unattributed
--
-- Notes:
--  - solana.transactions is read ONCE (Trino re-runs a CTE every time it is referenced), and is
--    joined on (id, block_slot, block_date) so the scan can skip row groups by slot and partition.
--  - Window filters use block_time; block_date is only a join key / partition filter for speed.
--  - Buffer addresses are discovered from create_buffer / initialize calls, so a minor version
--    bump of the program (which relocates every PDA) is picked up automatically.

with token_buffers as ( -- all token buffers ever created
    select distinct t.account as buffer_account
    from cow_protocol_solana.cow_settlement_call_create_buffer as cb
    -- args: [payer, system_program, token_program, (buffer, mint)...]
    cross join unnest(cb.call_account_arguments) with ordinality as t(account, pos)
    where t.pos >= 4 and t.pos % 2 = 0
),

buffer_mints as ( -- the mints those buffers hold (odd positions after the shared accounts)
    select distinct t.account as mint
    from cow_protocol_solana.cow_settlement_call_create_buffer as cb
    cross join unnest(cb.call_account_arguments) with ordinality as t(account, pos)
    where t.pos >= 5 and t.pos % 2 = 1
),

token_decimals as ( -- decimals for buffer mints only: keeps tokens_solana.fungible off the build side
    select
        token_mint_address,
        max(decimals) as decimals -- one row per mint, in case fungible ever holds duplicates
    from tokens_solana.fungible
    where token_mint_address in (select mint from buffer_mints)
    group by 1
),

native_buffers as ( -- native SOL buffer(s): created at Initialize, one per program version
    select distinct account_native_sol_buffer as buffer_account
    from cow_protocol_solana.cow_settlement_call_initialize
),

buf_list as ( -- buffers as arrays, for in-row filtering
    select
        (select array_agg(buffer_account) from native_buffers) as native_bufs,
        (
            select array_agg(buffer_account)
            from (
                select buffer_account from token_buffers
                union distinct
                select buffer_account from native_buffers
            ) as all_buffers
        ) as bufs
),

-- ---------- CoW txs in window (small decoded tables) ----------

settles as ( -- settlement txs (a tx may hold several settlements)
    select
        call_tx_id as tx_id,
        call_block_slot as block_slot,
        call_block_date as block_date,
        array_agg(distinct account_solver) as solvers,
        array_agg(auction_id order by call_outer_instruction_index) as auction_ids,
        count(*) as n_settlements
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
    group by 1, 2, 3
),

creates as ( -- txs with create_buffer (payer funds the rent)
    select
        call_tx_id as tx_id,
        call_block_slot as block_slot,
        call_block_date as block_date,
        min(account_payer) as payer
    from cow_protocol_solana.cow_settlement_call_create_buffer
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
    group by 1, 2, 3
),

protocol_txs as ( -- protocol-authority actions: reclaims and native SOL withdrawals
    select distinct
        call_tx_id as tx_id,
        call_block_slot as block_slot,
        call_block_date as block_date
    from cow_protocol_solana.cow_settlement_call_reclaim_buffer
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
    union distinct
    select distinct
        call_tx_id,
        call_block_slot,
        call_block_date
    from cow_protocol_solana.cow_settlement_call_withdraw_native_sol
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
),

cow_txs as (
    select
        tx_id,
        block_slot,
        block_date
    from settles
    union distinct
    select
        tx_id,
        block_slot,
        block_date
    from creates
    union distinct
    select
        tx_id,
        block_slot,
        block_date
    from protocol_txs
),

solver_events as ( -- solver registry changes (all history)
    select
        solver,
        call_block_time as event_time,
        1 as delta
    from cow_protocol_solana.cow_settlement_call_add_solver
    union all
    select
        solver,
        call_block_time as event_time,
        -1 as delta
    from cow_protocol_solana.cow_settlement_call_remove_solver
),

-- ---------- the single scan of solana.transactions ----------

tx_raw as (
    select
        t.id as tx_id,
        t.block_time,
        t.block_slot,
        t.signer,
        t.pre_token_balances,
        t.post_token_balances,
        t.pre_balances,
        t.post_balances,
        -- lamport arrays cover static + lookup-table keys; make sure keys do too
        case
            when cardinality(t.account_keys) = cardinality(t.pre_balances) then t.account_keys
            else concat(
                t.account_keys,
                coalesce(t.loaded_addresses.writable, cast(array[] as array(varchar))),
                coalesce(t.loaded_addresses.readonly, cast(array[] as array(varchar)))
            )
        end as keys
    from solana.transactions as t
    inner join cow_txs as c
        on c.tx_id = t.id
        and c.block_slot = t.block_slot -- lets the scan skip row groups whose slot range has no CoW tx
        and c.block_date = t.block_date
    where
        t.block_date between cast(cast('{{start_time}}' as timestamp) as date)
        and cast(cast('{{end_time}}' as timestamp) as date) -- partition pruning
        and t.block_time >= cast('{{start_time}}' as timestamp)
        and t.block_time < cast('{{end_time}}' as timestamp)
        and t.success
),

tx_entries as ( -- every buffer balance entry of each tx, signed so that sum = post - pre
    select
        r.tx_id,
        r.block_time,
        r.block_slot,
        r.signer,
        e.leg,
        e.buffer_account,
        e.mint,
        e.amount
    from tx_raw as r
    cross join buf_list as bl
    cross join unnest(
        concat(
            -- token balances (units): -pre, +post
            transform(
                filter(r.pre_token_balances, b -> contains(bl.bufs, b.account)),
                b -> cast(
                    row('token', b.account, b.mint, -b.amount)
                    as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            transform(
                filter(r.post_token_balances, b -> contains(bl.bufs, b.account)),
                b -> cast(
                    row('token', b.account, b.mint, b.amount)
                    as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            -- wSOL buffer: its lamports also move with wSOL tokens, so take those out
            -- of its lamport leg (+pre, -post, scaled to lamports; wSOL has 9 decimals)
            transform(
                filter(
                    r.pre_token_balances, b -> contains(bl.bufs, b.account)
                    and b.mint = 'So11111111111111111111111111111111111111112'
                ),
                b -> cast(
                    row('lamports', b.account, '11111111111111111111111111111111', b.amount * 1000000000)
                    as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            transform(
                filter(
                    r.post_token_balances, b -> contains(bl.bufs, b.account)
                    and b.mint = 'So11111111111111111111111111111111111111112'
                ),
                b -> cast(
                    row('lamports', b.account, '11111111111111111111111111111111', -(b.amount * 1000000000))
                    as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            -- lamport change of every buffer present in the tx
            transform(
                filter(sequence(1, cardinality(r.keys)), i -> contains(bl.bufs, r.keys[i])),
                i -> cast(row(
                    if(contains(bl.native_bufs, r.keys[i]), 'native_sol', 'lamports'),
                    r.keys[i],
                    '11111111111111111111111111111111',
                    r.post_balances[i] - r.pre_balances[i]
                )
                as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18)))
            )
        )
    ) as e(leg, buffer_account, mint, amount)
),

legs as ( -- net change per tx, buffer, leg and token
    select
        tx_id,
        block_time,
        block_slot,
        signer,
        buffer_account,
        leg,
        mint,
        sum(amount) as amount_units
    from tx_entries
    group by 1, 2, 3, 4, 5, 6, 7
),

-- ---------- attribution (small joins only) ----------

joined as (
    select
        l.*,
        s.solvers,
        s.auction_ids,
        s.n_settlements,
        cr.tx_id is not null as creates_buffer,
        p.tx_id is not null as is_protocol,
        coalesce(cr.payer, l.signer) as candidate
    from legs as l
    left join settles as s on s.tx_id = l.tx_id
    left join creates as cr on cr.tx_id = l.tx_id
    left join (select distinct tx_id from protocol_txs) as p on p.tx_id = l.tx_id
),

with_status as ( -- non-settlement: was the rent payer (else signer) a registered solver then?
    select
        j.*,
        coalesce((
            select sum(e.delta)
            from solver_events as e
            where
                e.solver = j.candidate
                and e.event_time <= j.block_time
        ), 0) > 0 as candidate_is_solver
    from joined as j
)

select
    w.tx_id,
    w.block_time,
    w.block_slot,
    case
        when w.solvers is not null and cardinality(w.solvers) = 1 then 'settlement'
        when w.solvers is not null then 'multi_solver'
        when w.is_protocol then 'protocol'
        when w.candidate_is_solver then 'solver_non_settlement'
        else 'unattributed'
    end as attribution,
    case
        when w.solvers is not null and cardinality(w.solvers) = 1 then w.solvers[1]
        when w.solvers is null and not w.is_protocol and w.candidate_is_solver then w.candidate
    end as solver,
    w.auction_ids,
    w.n_settlements,
    w.creates_buffer,
    w.buffer_account,
    w.leg,
    w.mint as token_address,
    case
        when w.leg = 'token'
            then cast(w.amount_units * cast(power(10, d.decimals) as decimal(20,0)) as decimal(38,0))
        else cast(w.amount_units as decimal(38,0)) -- lamports are already atoms
    end as amount
from with_status as w
left join token_decimals as d
    on w.leg = 'token'
    and d.token_mint_address = w.mint
where
    case
        when w.leg = 'token'
            then cast(w.amount_units * cast(power(10, d.decimals) as decimal(20,0)) as decimal(38,0))
        else cast(w.amount_units as decimal(38,0))
    end is distinct from 0 -- keeps null amounts (missing decimals) visible
order by w.block_time desc, w.tx_id, w.buffer_account

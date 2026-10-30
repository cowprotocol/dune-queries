-- CoW settlement (prod): buffer imbalances per tx, with solver attribution.
-- Txs = every tx with a CoW settlement / create_buffer / reclaim_buffer / withdraw_native_sol call.
-- solana.transactions is read ONCE (Trino re-runs a CTE every time it is referenced).
-- Window filters use block_time; block_date is only a join key / partition filter for speed.
-- Legs:
--   token      : token balance change of a buffer (atoms)
--   native_sol : lamport change of the native SOL buffer (its balance is SOL)
--   lamports   : lamport change of a token buffer = rent on creation / reclaim,
--                net of wSOL token movements on the wSOL buffer (no double count)

with buffers as ( -- all token buffers ever created + the native SOL buffer
    select distinct t.account as buffer_account
    from cow_protocol_solana.cow_settlement_call_create_buffer as cb
    -- args: [payer, system_program, token_program, (buffer, mint)...]
    cross join unnest(cb.call_account_arguments) with ordinality as t(account, pos)
    where t.pos >= 4 and t.pos % 2 = 0
    union
    select 'ESnpC8iNkuUzbLRm2dG6VezaGBi86QPdYh6TzvSXn2Pz' -- native SOL buffer (fixed PDA, created at Initialize)
),

buf_list as ( -- buffers as one array, for in-row filtering
    select array_agg(buffer_account) as bufs
    from buffers
),

-- ---------- CoW txs in window (small decoded tables) ----------

settles as ( -- settlement txs (a tx may hold several settlements)
    select
        call_tx_id as tx_id,
        call_block_date as block_date,
        array_agg(distinct account_solver) as solvers,
        array_agg(auction_id order by call_outer_instruction_index) as auction_ids,
        count(*) as n_settlements
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
    group by 1, 2
),

creates as ( -- txs with create_buffer (payer funds the rent)
    select
        call_tx_id as tx_id,
        call_block_date as block_date,
        min(account_payer) as payer
    from cow_protocol_solana.cow_settlement_call_create_buffer
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
    group by 1, 2
),

protocol_txs as ( -- protocol-authority actions: reclaims and native SOL withdrawals
    select distinct call_tx_id as tx_id, call_block_date as block_date
    from cow_protocol_solana.cow_settlement_call_reclaim_buffer
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
    union
    select distinct call_tx_id, call_block_date
    from cow_protocol_solana.cow_settlement_call_withdraw_native_sol
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time <= cast('{{end_time}}' as timestamp)
),

cow_txs as (
    select tx_id, block_date from settles
    union
    select tx_id, block_date from creates
    union
    select tx_id, block_date from protocol_txs
),

solver_events as ( -- solver registry changes (all history)
    select solver, call_block_time as event_time, 1 as delta
    from cow_protocol_solana.cow_settlement_call_add_solver
    union all
    select solver, call_block_time, -1
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
        and c.block_date = t.block_date
    where
        t.block_date between cast(cast('{{start_time}}' as timestamp) as date)
                         and cast(cast('{{end_time}}' as timestamp) as date) -- partition pruning
        and t.block_time >= cast('{{start_time}}' as timestamp)
        and t.block_time <= cast('{{end_time}}' as timestamp)
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
                b -> cast(row('token', b.account, b.mint, -b.amount)
                          as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18)))
            ),
            transform(
                filter(r.post_token_balances, b -> contains(bl.bufs, b.account)),
                b -> cast(row('token', b.account, b.mint, b.amount)
                          as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18)))
            ),
            -- wSOL buffer: its lamports also move with wSOL tokens, so take those out
            -- of its lamport leg (+pre, -post, scaled to lamports; wSOL has 9 decimals)
            transform(
                filter(r.pre_token_balances, b -> contains(bl.bufs, b.account)
                                                  and b.mint = 'So11111111111111111111111111111111111111112'),
                b -> cast(row('lamports', b.account, '11111111111111111111111111111111', b.amount * 1000000000)
                          as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18)))
            ),
            transform(
                filter(r.post_token_balances, b -> contains(bl.bufs, b.account)
                                                   and b.mint = 'So11111111111111111111111111111111111111112'),
                b -> cast(row('lamports', b.account, '11111111111111111111111111111111', -(b.amount * 1000000000))
                          as row(leg varchar, buffer_account varchar, mint varchar, amount decimal(38,18)))
            ),
            -- lamport change of every buffer present in the tx
            transform(
                filter(sequence(1, cardinality(r.keys)), i -> contains(bl.bufs, r.keys[i])),
                i -> cast(row(
                        if(r.keys[i] = 'ESnpC8iNkuUzbLRm2dG6VezaGBi86QPdYh6TzvSXn2Pz', 'native_sol', 'lamports'),
                        r.keys[i],
                        '11111111111111111111111111111111',
                        r.post_balances[i] - r.pre_balances[i])
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
            where e.solver = j.candidate
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
            then cast(w.amount_units * cast(power(10, f.decimals) as decimal(20,0)) as decimal(38,0))
        else cast(w.amount_units as decimal(38,0)) -- lamports are already atoms
    end as amount
from with_status as w
left join tokens_solana.fungible as f
    on w.leg = 'token'
    and f.token_mint_address = w.mint
where
    case
        when w.leg = 'token'
            then cast(w.amount_units * cast(power(10, f.decimals) as decimal(20,0)) as decimal(38,0))
        else cast(w.amount_units as decimal(38,0))
    end is distinct from 0 -- keeps null amounts (missing decimals) visible
order by w.block_time desc, w.tx_id, w.buffer_account

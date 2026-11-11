-- CoW settlement (prod): buffer imbalances per tx, with solver attribution.
--
-- Solana analogue of the EVM token-imbalance queries (4021257 / 4021644). Unlike the EVM
-- version it does NOT look at every transfer touching the buffers: it only scans txs that
-- contain begin_settle (settlements) or create_buffer (buffer creations, by solvers or users).
-- Out of scope: buffer in-flows made in other txs (e.g. a solver pre-funding a buffer), and
-- protocol actions (reclaim_buffer, withdraw_native_sol), which involve no solver.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}}   - the timestamp for which the analysis should end (exclusively)
--
-- Output: one row per (tx, buffer, token) whose balance changed.
--
--   token_address  the token whose balance changed:
--                    a mint      the buffer's token balance, e.g. USDC held by the USDC buffer
--                    111...1     SOL (lamports). On the native SOL buffer, this is its SOL
--                                imbalance: that buffer is a plain account holding SOL
--                                directly. On a token buffer, it's the rent paid when the
--                                buffer is created: SOL sitting in the account itself, not part
--                                of its tokens.
--                  The wSOL buffer holds its wSOL tokens as lamports too, so wSOL movements are
--                  subtracted from its 111...1 row: wSOL is counted once (So111...2 row) and the
--                  111...1 row keeps only the rent.
--
--   amount         post - pre, in atoms (smallest unit: token base units, or lamports for 111...1).
--                  NULL if the token's decimals are unknown (mint missing from
--                  tokens_solana.fungible); these rows are kept so they can be fixed.
--
--   solver         who the change is attributed to:
--                    settlement txs   the account_solver of its begin_settle
--                    other txs        the rent payer (else the signer), if a whitelisted solver
--                                     at the time, e.g. a solver creating a buffer outside a
--                                     settlement
--                    null             no solver involved, e.g. a user creating a buffer when
--                                     placing an order
--
-- Assertions (the query fails instead of returning wrong data):
--  - a buffer mint has more than one row in tokens_solana.fungible
--  - a settlement tx has more than one solver
--  - start_time is before the data covered by the materialized view
--
-- Notes:
--  - Balances come from the materialized view dune.cowprotocol.result_solana_txs_past_10_days_materialized
--    (raw balances of CoW txs in the last 10 days). Make sure it was refreshed after end_time.
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

token_decimals as ( -- decimals for buffer mints only; fails if fungible has more than one row for a mint
    select
        token_mint_address,
        case
            when count(*) > 1
                then
                    cast(
                        'ASSERTION FAILED: multiple tokens_solana.fungible rows for mint ' || token_mint_address
                        as integer
                    )
            else max(decimals)
        end as decimals
    from tokens_solana.fungible
    where token_mint_address in (select mint from buffer_mints)
    group by 1
),

native_buffers as ( -- native SOL buffer(s): created at Initialize, one per program version
    select distinct account_native_sol_buffer as buffer_account
    from cow_protocol_solana.cow_settlement_call_initialize
),

buf_list as ( -- all buffers (token + native SOL) as one array, for in-row filtering
    select array_agg(buffer_account) as bufs
    from (
        select buffer_account from token_buffers
        union distinct
        select buffer_account from native_buffers
    ) as all_buffers
),

-- ---------- CoW txs in window (small decoded tables) ----------

settles as ( -- settlement txs (a tx may hold several settlements)
    select
        call_tx_id as tx_id,
        array_agg(distinct account_solver) as solvers,
        array_agg(auction_id order by call_outer_instruction_index) as auction_ids,
        count(*) as n_settlements
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
    group by 1
),

creates as ( -- txs with create_buffer (payer funds the rent)
    select
        call_tx_id as tx_id,
        min(account_payer) as payer
    from cow_protocol_solana.cow_settlement_call_create_buffer
    where
        call_block_time >= cast('{{start_time}}' as timestamp)
        and call_block_time < cast('{{end_time}}' as timestamp)
    group by 1
),

cow_txs as ( -- the txs in scope: settlements and buffer creations
    select tx_id from settles
    union distinct
    select tx_id from creates
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

-- ---------- assertions ----------

solver_check as ( -- fails if a settlement tx has more than one solver (solver would be ambiguous)
    select
        case
            when count_if(cardinality(solvers) > 1) > 0
                then cast('ASSERTION FAILED: settlement tx with more than one solver' as integer)
            else 1
        end as ok
    from settles
),

coverage_check as ( -- fails if the window starts before the materialized view's data
    select
        case
            when cast('{{start_time}}' as timestamp) < min(block_time)
                then cast('ASSERTION FAILED: start_time is before the materialized view coverage' as integer)
            else 1
        end as ok
    from dune.cowprotocol.result_solana_txs_past_10_days_materialized
),

-- ---------- raw balances of the CoW txs (materialized view) ----------

tx_raw as (
    select
        t.tx_id,
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
    from dune.cowprotocol.solana_cow_transactions as t
    where
        t.block_time >= cast('{{start_time}}' as timestamp)
        and t.block_time < cast('{{end_time}}' as timestamp)
        and t.tx_id in (select tx_id from cow_txs) -- settlements and buffer creations only
),

tx_entries as ( -- every buffer balance entry of each tx, signed so that sum = post - pre
    select
        r.tx_id,
        r.block_time,
        r.block_slot,
        r.signer,
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
                    row(b.account, b.mint, -b.amount)
                    as row(buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            transform(
                filter(r.post_token_balances, b -> contains(bl.bufs, b.account)),
                b -> cast(
                    row(b.account, b.mint, b.amount)
                    as row(buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            -- wSOL correction. On the wSOL buffer, wSOL tokens are held as lamports, so every
            -- wSOL movement also shows up in its lamport change (last group below). Adding the
            -- wSOL token balances to its 111...1 entries with the opposite sign (+pre, -post,
            -- scaled to lamports; wSOL has 9 decimals) cancels that out, so the buffer's 111...1
            -- row keeps only rent and the wSOL movement is counted once, in its So111...2 row.
            transform(
                filter(
                    r.pre_token_balances, b -> contains(bl.bufs, b.account)
                    and b.mint = 'So11111111111111111111111111111111111111112'
                ),
                b -> cast(
                    row(b.account, '11111111111111111111111111111111', b.amount * 1000000000)
                    as row(buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            transform(
                filter(
                    r.post_token_balances, b -> contains(bl.bufs, b.account)
                    and b.mint = 'So11111111111111111111111111111111111111112'
                ),
                b -> cast(
                    row(b.account, '11111111111111111111111111111111', -(b.amount * 1000000000))
                    as row(buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            ),
            -- lamport change of every buffer present in the tx (recorded as 111...1, in lamports)
            transform(
                filter(sequence(1, cardinality(r.keys)), i -> contains(bl.bufs, r.keys[i])),
                i -> cast(
                    row(r.keys[i], '11111111111111111111111111111111', r.post_balances[i] - r.pre_balances[i])
                    as row(buffer_account varchar, mint varchar, amount decimal(38,18))
                )
            )
        )
    ) as e(buffer_account, mint, amount)
),

changes as ( -- net change per tx, buffer and token (units for mints, lamports for 111...1)
    select
        tx_id,
        block_time,
        block_slot,
        signer,
        buffer_account,
        mint,
        sum(amount) as amount_units
    from tx_entries
    group by 1, 2, 3, 4, 5, 6
),

-- ---------- attribution (small joins only) ----------

joined as (
    select
        c.*,
        s.solvers,
        s.auction_ids,
        s.n_settlements,
        cr.tx_id is not null as creates_buffer,
        coalesce(cr.payer, c.signer) as candidate
    from changes as c
    left join settles as s on s.tx_id = c.tx_id
    left join creates as cr on cr.tx_id = c.tx_id
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
),

with_amounts as ( -- convert token units to atoms (lamports are already atoms)
    select
        w.*,
        case
            when w.mint = '11111111111111111111111111111111' then cast(w.amount_units as decimal(38,0))
            else cast(w.amount_units * cast(power(10, d.decimals) as decimal(20,0)) as decimal(38,0))
        end as amount
    from with_status as w
    left join token_decimals as d
        on d.token_mint_address = w.mint
)

select
    a.tx_id,
    a.block_time,
    a.block_slot,
    case
        when a.solvers is not null then a.solvers[1] -- settlement: its account_solver
        when a.candidate_is_solver then a.candidate  -- e.g. a solver creating a buffer outside a settlement
    end as solver,                                   -- null: no solver involved (e.g. a user creating a buffer)
    a.auction_ids,
    a.n_settlements,
    a.creates_buffer,
    a.buffer_account,
    a.mint as token_address,
    a.amount
from with_amounts as a
cross join solver_check
cross join coverage_check
where a.amount is distinct from 0 -- keeps null amounts (missing decimals) visible
order by a.block_time desc, a.tx_id, a.buffer_account

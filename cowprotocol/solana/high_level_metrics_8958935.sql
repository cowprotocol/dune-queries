-- CoW Protocol on Solana: swaps, orders, traders and volume per {{granularity}}
-- source: cow_protocol_solana decoded tables for the prod settlement program (v0.5.x)
--   create_order is fully decoded; begin_settle / finalize_settle only decode their fixed args,
--   so per-order amounts and accounts are parsed from call_data / call_account_arguments
-- decoded tables only hold successful transactions
-- a swap = one order filled in a begin_settle instruction (partial fills count once per fill)
-- all CTEs read the full history since launch (2026-10-07) so that new traders and cumulative totals
-- are correct; {{start_time}} only limits which periods are returned
-- prices.minute is read once (it is most of the query cost): each fill is split into a sell and a buy leg,
-- both legs are priced in one join, then put back into one row
with begin_settle as (
    select
        call_block_time as block_time
        , call_tx_id as tx_id
        , call_data as data
        , call_account_arguments as account_arguments
        -- data: [disc u8][finalize_ix_index u16][auction_id i64][order_count u8][transfer_count u8 x order_count][amount u64 LE x sum(transfer_count)]
        , bytearray_to_bigint(bytearray_substring(call_data, 12, 1)) as order_count
    from cow_protocol_solana.cow_settlement_call_begin_settle
    where call_executing_account = 'Moook87DzJ25dELx3LXJ3dnu5a4ERVFwKo1PWWF4p7Y'
        and call_block_time >= timestamp '2026-10-07'
)

, begin_orders as (
    select
        b.block_time
        , b.tx_id
        , b.data
        , b.account_arguments
        , b.order_count
        , o.i as order_index
        , transform(sequence(1, b.order_count), n -> bytearray_to_bigint(bytearray_substring(b.data, 12 + n, 1))) as transfer_counts
    from begin_settle b
    cross join unnest(sequence(1, b.order_count)) as o(i)
)

, pulls as (
    select
        block_time
        , tx_id
        , order_index
        , prior_transfers
        -- accounts: 5 fixed, then per order [order_pda, sell_token_account, sell_mint, destination x transfer_count]
        , account_arguments[6 + 3 * (order_index - 1) + prior_transfers] as order_pda
        , reduce(
            sequence(0, transfer_counts[order_index] - 1)
            , cast(0 as double)
            , (s, k) -> s + cast(bytearray_to_uint256(bytearray_reverse(
                bytearray_substring(data, 13 + order_count + 8 * (prior_transfers + k), 8)
            )) as double)
            , s -> s
        ) as sell_amount_raw
    from (
        select
            *
            , reduce(slice(transfer_counts, 1, order_index - 1), cast(0 as bigint), (s, x) -> s + x, s -> s) as prior_transfers
        from begin_orders
    )
)

, finalize_settle as (
    select
        call_tx_id as tx_id
        , call_data as data
        , call_account_arguments as account_arguments
        -- data: [disc u8][begin_ix_index u16][bump u8 x push_count][amount u64 LE x push_count]
        -- accounts: 4 fixed, then per push [source_buffer, destination, mint]
        , (cardinality(call_account_arguments) - 4) / 3 as push_count
    from cow_protocol_solana.cow_settlement_call_finalize_settle
    where call_executing_account = 'Moook87DzJ25dELx3LXJ3dnu5a4ERVFwKo1PWWF4p7Y'
        and call_block_time >= timestamp '2026-10-07'
)

, pushes as (
    select
        f.tx_id
        , f.account_arguments[4 + 3 * (p.j - 1) + 2] as destination
        , cast(bytearray_to_uint256(bytearray_reverse(
            bytearray_substring(f.data, 4 + f.push_count + 8 * (p.j - 1), 8)
        )) as double) as buy_amount_raw
    from finalize_settle f
    cross join unnest(sequence(1, f.push_count)) as p(j)
)

, orders as (
    -- intent is a JSON string; addresses come as "PublicKey(<base58>)"
    -- order_pda is derived from the intent hash, so all fields are the same for a given order_pda
    select distinct
        account_order_pda as order_pda
        , regexp_extract(json_extract_scalar(intent, '$.OrderIntent.owner'), 'PublicKey\((\w+)\)', 1) as owner
        , regexp_extract(json_extract_scalar(intent, '$.OrderIntent.sell_mint'), 'PublicKey\((\w+)\)', 1) as sell_mint
        , regexp_extract(json_extract_scalar(intent, '$.OrderIntent.buy_token_account'), 'PublicKey\((\w+)\)', 1) as buy_token_account
        -- native SOL buys use the system program as buy_mint; map it to wSOL for pricing
        , if(
            regexp_extract(json_extract_scalar(intent, '$.OrderIntent.buy_mint'), 'PublicKey\((\w+)\)', 1) = '11111111111111111111111111111111'
            , 'So11111111111111111111111111111111111111112'
            , regexp_extract(json_extract_scalar(intent, '$.OrderIntent.buy_mint'), 'PublicKey\((\w+)\)', 1)
        ) as buy_mint
    from cow_protocol_solana.cow_settlement_call_create_order
    where call_executing_account = 'Moook87DzJ25dELx3LXJ3dnu5a4ERVFwKo1PWWF4p7Y'
        and call_block_time >= timestamp '2026-10-07'
)

, fills as (
    -- one row per filled order, with both amounts and both mints
    select
        pl.block_time
        , pl.tx_id
        , pl.order_index
        , pl.order_pda
        , o.owner
        , o.sell_mint
        , o.buy_mint
        , pl.sell_amount_raw
        , pu.buy_amount_raw
    from pulls pl
    join orders o on o.order_pda = pl.order_pda
    left join pushes pu
        on pu.tx_id = pl.tx_id
        and pu.destination = o.buy_token_account
)

, priced_fills as (
    -- split each fill into a sell leg and a buy leg, price both legs with one read of prices.minute,
    -- then put each fill back into one row
    select
        l.block_time
        , l.tx_id
        , l.order_index
        , l.order_pda
        , l.owner
        , max(if(l.side = 'sell', l.sell_amount_raw / power(10, p.decimals) * p.price)) as sell_usd
        , max(if(l.side = 'buy', l.buy_amount_raw / power(10, p.decimals) * p.price)) as buy_usd
    from (
        select
            f.*
            , s.side
            , s.mint
        from fills f
        cross join unnest(array['sell', 'buy'], array[f.sell_mint, f.buy_mint]) as s(side, mint)
    ) l
    left join (
        select
            timestamp
            , contract_address_varchar as mint
            , price
            , decimals
        from prices.minute
        where blockchain = 'solana'
            and timestamp >= timestamp '2026-10-07'
    ) p
        on p.mint = l.mint
        and p.timestamp = date_trunc('minute', l.block_time)
    group by 1, 2, 3, 4, 5
)

, trades as (
    select
        date_trunc('{{granularity}}', block_time) as period
        , tx_id
        , order_pda
        , owner
        -- sell side first; buy side when the sell token has no price
        , coalesce(sell_usd, buy_usd) as volume_usd
        -- a trader is new in the period of their first swap on the prod program
        , min(date_trunc('{{granularity}}', block_time)) over (partition by owner) as first_period
    from priced_fills
)

, by_period as (
    select
        period
        , sum(volume_usd) as volume_usd
        , count(*) as swaps
        -- a partially fillable order filled several times in a period counts once here
        , count(distinct order_pda) as orders_filled
        , count(distinct owner) as traders
        , count(distinct case when first_period = period then owner end) as new_traders
        , count(distinct tx_id) as settlements
        , count_if(volume_usd is null) as unpriced_swaps
    from trades
    group by 1
)

, with_cumulative as (
    select
        period
        , volume_usd
        , sum(volume_usd) over (order by period) as cumulative_volume_usd
        , swaps
        , sum(swaps) over (order by period) as cumulative_swaps
        , orders_filled
        , traders
        , new_traders
        , traders - new_traders as returning_traders
        , settlements
        , unpriced_swaps
    from by_period
)

select *
from with_cumulative
where period >= date_trunc('{{granularity}}', timestamp '{{start_time}}')
order by period desc

-- This query used incompatible data types from Dune SQL alpha and may need to be updated.
-- More details can be found on https://dune.com/docs/query/dunesql-changes/
WITH solver_info as (
    SELECT 
        name as solver_name,
        -- concat(environment, '-', name) as name,
        max(block_time) last_solution,
        count(*) as batches_solved,
        sum(dex_swaps) as dex_swaps,
        sum(num_trades) as num_trades,
        sum(gas_used) as gas_used,
        sum(batch_value) as total_batch_value,
        avg(batch_value) as average_batch_volume,
        avg(num_trades) as average_batch_size,
        1.0 * sum(gas_used) / sum(num_trades) as average_gas_per_trade,
        1.0 * sum(dex_swaps) / sum(num_trades) as average_dex_swaps_per_trade
    FROM cow_protocol_{{blockchain}}.batches b
    JOIN cow_protocol_{{blockchain}}.solvers 
        ON solver_address = address
    WHERE environment not in ('test', 'service')
    and block_date > now() - interval '{{LastNDays}}' day
    and active = True
    GROUP BY name
    ORDER BY num_trades DESC
)

-- --- Next for Surplus Computation
-- batches_with_nested_uids_and_trades AS (
--     SELECT call_tx_hash,
--         array_agg(
--             "orderUid"
--             ORDER BY evt_index ASC
--         ) as uids,
--         (
--             select jsonb_agg(t->'sellAmount')
--             from jsonb_array_elements(trades) as x(t)
--         ) as sell_amount_limits,
--         (
--             select jsonb_agg(t->'buyAmount')
--             from jsonb_array_elements(trades) as x(t)
--         ) as buy_amount_limits,
--         (
--             select jsonb_agg(t->'sellTokenIndex')
--             from jsonb_array_elements(trades) as x(t)
--         ) as sell_token_indices,
--         (
--             select jsonb_agg(t->'buyTokenIndex')
--             from jsonb_array_elements(trades) as x(t)
--         ) as buy_token_indices,
--         (
--             select jsonb_agg(t->'executedAmount')
--             from jsonb_array_elements(trades) as x(t)
--         ) as executed_amounts,
--         (
--             select jsonb_agg(t->'flags')
--             from jsonb_array_elements(trades) as x(t)
--         ) as order_flags -- tokens,
--         -- "clearingPrices" as prices
--     FROM gnosis_protocol_v2."GPv2Settlement_call_settle"
--         JOIN gnosis_protocol_v2."GPv2Settlement_evt_Trade" ON call_tx_hash = evt_tx_hash
--     GROUP BY call_tx_hash,
--         trades
-- ),
-- uid_to_limit_prices AS (
--     SELECT call_tx_hash as tx_hash,
--         unnest(uids) as uid,
--         jsonb_array_elements(sell_amount_limits)::numeric as sell_amount_limit,
--         jsonb_array_elements(buy_amount_limits)::numeric as buy_amount_limit,
--         jsonb_array_elements(buy_token_indices)::numeric as buy_token_index,
--         jsonb_array_elements(sell_token_indices)::numeric as sell_token_index,
--         -- jsonb_array_elements(executed_amounts)::numeric as executed_amount,
--         jsonb_array_elements(order_flags)::integer & 1 as order_kind
--     FROM batches_with_nested_uids_and_trades
-- ),
-- limit_with_executed_amounts as (
--     select evt_block_time,
--         tx_hash,
--         uid,
--         owner,
--         order_kind,
--         "buyToken" as buy_token,
--         "sellToken" as sell_token,
--         -- Limit amounts
--         buy_amount_limit,
--         sell_amount_limit,
--         -- executed amounts
--         "buyAmount" as executed_buy_amount,
--         ("sellAmount" - "feeAmount") as executed_sell_amount,
--         "feeAmount" as fee
--     from gnosis_protocol_v2."GPv2Settlement_evt_Trade"
--         inner join uid_to_limit_prices on uid = "orderUid"
-- ),
-- clearing_prices as (
--     SELECT call_tx_hash,
--         unnest(tokens) as token,
--         unnest("clearingPrices") as price
--     FROM gnosis_protocol_v2."GPv2Settlement_call_settle"
-- ),
-- trades_with_prices as (
--     SELECT evt_block_time,
--         tx_hash,
--         uid,
--         owner,
--         order_kind,
--         buy_token,
--         sell_token,
--         buy_amount_limit,
--         sell_amount_limit,
--         executed_buy_amount,
--         executed_sell_amount,
--         b.price::numeric as buy_token_price,
--         s.price::numeric as sell_token_price,
--         fee
--     FROM limit_with_executed_amounts
--         JOIN clearing_prices b on tx_hash = b.call_tx_hash
--         and buy_token = b.token
--         JOIN clearing_prices s on tx_hash = s.call_tx_hash
--         and sell_token = s.token
-- ),
-- trades_with_surplus as (
--     select evt_block_time,
--         owner as trader,
--         case
--             when order_kind = 1 then 'BUY'
--             else 'SELL'
--         end as order_kind,
--         s.symbol as sell_token,
--         b.symbol as buy_token,
--         sell_amount_limit / pow(10, s.decimals) as sell_amount_limit,
--         buy_amount_limit / pow(10, b.decimals) as buy_amount_limit,
--         executed_sell_amount / pow(10, s.decimals) as executed_sell_amount,
--         executed_buy_amount / pow(10, b.decimals) as executed_buy_amount,
--         fee / pow(10, s.decimals) as fee,
--         100.0 * (
--             (sell_amount_limit * sell_token_price) / (buy_amount_limit * buy_token_price) - 1
--         ) as surplus_ratio,
--         case
--             when order_kind = 1 -- buy order
--             then (
--                 (
--                     executed_buy_amount * sell_amount_limit / buy_amount_limit * sell_token_price
--                 ) - (executed_buy_amount * buy_token_price)
--             ) / (sell_token_price * pow(10, s.decimals)) -- sell order
--             else (
--                 (executed_sell_amount * sell_token_price) - (
--                     executed_sell_amount * buy_amount_limit / sell_amount_limit * buy_token_price
--                 )
--             ) / (buy_token_price * pow(10, b.decimals))
--         end as absolute_surplus,
--         -- This is in the relevant token sell_token for buy orders, and buy token for sell orders
--         tx_hash,
--         uid as order_id
--     from trades_with_prices
--         join erc20.tokens b on buy_token = b.contract_address
--         join erc20.tokens s on sell_token = s.contract_address
-- ),
-- trade_surpluses as (
--     SELECT block_time,
--         t.tx_hash,
--         round(
--             CASE
--                 WHEN order_kind = 'BUY' THEN absolute_surplus * trade_value_usd / executed_sell_amount
--                 ELSE absolute_surplus * trade_value_usd / executed_buy_amount
--             END::numeric,
--             2
--         ) as surplus_usd
--     FROM gnosis_protocol_v2.trades t
--         JOIN trades_with_surplus s on t.tx_hash = s.tx_hash
--         and order_uid = order_id -- where surplus_ratio < 10
--     where t.tx_hash != '\x17f8153f4e5ca2299de848b99b3df3e0cf4a22610aa20a1312e8651a637681bf' -- Division by Zero
-- ),
-- batchwise_surplus_results as (
--     select s.block_time,
--         s.tx_hash,
--         solver_name,
--         sum(surplus_usd) as batch_surplus,
--         batch_value
--     from trade_surpluses s
--         join gnosis_protocol_v2.batches b on b.tx_hash = s.tx_hash
--     group by s.block_time,
--         s.tx_hash,
--         solver_name,
--         batch_value
-- ),
-- surplus_results as (
--     select solver_name,
--         avg(batch_surplus) average_surplus
--     from batchwise_surplus_results
--     group by solver_name
-- )
select ROW_NUMBER() OVER (
        ORDER BY average_gas_per_trade
    ) AS rk,
    si.*
    -- average_surplus
from solver_info si
    -- join surplus_results sr on si.solver_name = sr.solver_name
order by rk 

-- This query returns prices for tokens traded via CoW Protocol
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The returned table has columns:
-- - block_time: time a a settlement transaction
-- - tx_hash: settlement transaction hash
-- - token_address:
-- - token_price: price in USD of one _atom_ of a token
--
-- Prices are either fetched from the trades table which contains Dune prices if they exist,
-- or computes them from the exchange rate from the trade if the second traded token has a price on Dune.
-- If no trade with a dune price exists for a token, the price is set to zero.
--
-- Prices are in USD _per atom_ to avoid special casing of trades involving tokens without an entry in erc20 tables.
-- This fits to naturally to other queries using amounts in atoms. If amounts in _units_ of a token are used,
-- prices need to be scaled using decimals of the token.

with filtered_trades as (
    select
        *
    from cow_protocol_{{blockchain}}.trades
    where block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
),

token_prices_from_trades as (
    select
        block_time,
        tx_hash,
        sell_token_address,
        buy_token_address,
        order_uid,
        sell_price * units_sold / atoms_sold as token_price_sell, -- in usd per atom
        buy_price * units_bought / atoms_bought as token_price_buy,
        buy_price * units_bought / atoms_bought * atoms_bought / atoms_sold  as token_price_backup_sell,
        sell_price * units_sold / atoms_sold * atoms_sold / atoms_bought  as token_price_backup_buy
    from filtered_trades
),
token_prices_from_trades_sell as (
    select
        block_time,
        tx_hash,
        sell_token_address as token_address,
        token_price_sell as token_price,
        token_price_backup_sell as token_price_backup
    from token_prices_from_trades
),
token_prices_from_trades_buy as (
    select
        block_time,
        tx_hash,
        buy_token_address as token_address,
        token_price_buy as token_price,
        token_price_backup_buy as token_price_backup
    from token_prices_from_trades
),
token_prices_all as (
    select * from token_prices_from_trades_sell
    union all
    select * from token_prices_from_trades_buy
)

select
    block_time,
    tx_hash,
    token_address,
    coalesce(max(token_price), avg(token_price_backup), 0) as token_price
from token_prices_all
group by block_time, tx_hash, token_address

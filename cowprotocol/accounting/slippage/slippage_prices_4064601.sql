-- This query computes prices for use in slippage on CoW Protocol
--
-- It gives hourly prices for all tokens transferred to or from the protocol.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are
-- - hour: hour for which a price is valid
-- - token_address: address of token with a price. contract address for erc20 tokens,
--   0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee for native token
-- - decimals: decimals of the token, can be null
-- - price_unit: USD price of one unit (i.e. pow(10, decimals) atoms) of a token
-- - price_atom: USD price of one atom (i.e. 1. / pow(10, decimals) units) of a token

-- Fetch a list of token addresses and times we need prices for. We use hourly prices only.
with token_times as (
    select
        token_address,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4021257(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
    where token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    group by 1, 2
),

-- Precise prices are prices from the Dune price feed.
precise_prices as (
    select -- noqa: ST06
        date_trunc('hour', minute) as hour, --noqa: RF04
        token_address,
        decimals,
        avg(price) as price_unit,
        avg(price) / pow(10, decimals) as price_atom
    from
        prices.usd
    inner join token_times
        on
            date_trunc('hour', minute) = hour
            and contract_address = token_address
            and blockchain = '{{blockchain}}'
    group by 1, 2, 3
),

-- Intrinsic prices are prices reconstructed from exchange rates from within the auction
-- A price can be reconstructed if there was a trade with another token which did have a Dune price.
-- If there a multiple prices reconstructed in this way, an average is taken.
-- The native token is excluded from this analysis since we will explicitly get a price for that later.
intrinsic_prices as (
    select
        hour,
        token_address,
        decimals,
        avg(price_unit) as price_unit,
        avg(price_atom) as price_atom
    from (
        select
            date_trunc('hour', block_time) as hour, --noqa: RF04
            buy_token_address as token_address,
            round(log(10, atoms_bought / units_bought)) as decimals,
            usd_value / units_bought as price_unit,
            usd_value / atoms_bought as price_atom
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and buy_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        union distinct
        select
            date_trunc('hour', block_time) as hour, --noqa: RF04
            sell_token_address as token_address,
            round(log(10, atoms_sold / units_sold)) as decimals,
            usd_value / units_sold as price_unit,
            usd_value / atoms_sold as price_atom
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and sell_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    ) as combined
    group by 1, 2, 3
),

-- The final price is the Dune price if it exists and the intrinsic price otherwise. If both prices
-- are not available, the price is null.
prices_pre as (
    select
        tt.hour,
        tt.token_address,
        coalesce(
            precise.decimals,
            intrinsic.decimals
        ) as decimals,
        coalesce(
            precise.price_unit,
            intrinsic.price_unit
        ) as price_unit,
        coalesce(
            precise.price_atom,
            intrinsic.price_atom
        ) as price_atom
    from token_times as tt
    left join precise_prices as precise
        on
            tt.hour = precise.hour
            and tt.token_address = precise.token_address
    left join intrinsic_prices as intrinsic
        on
            tt.hour = intrinsic.hour
            and tt.token_address = intrinsic.token_address
),

prices as (
    select
        hour,
        token_address,
        decimals,
        case
            when '{{blockchain}}' = 'base' and token_address = 0x22af33fe49fd1fa80c7149773dde5890d3c76f3b and hour >= timestamp '2025-03-04 00:00' and hour <= timestamp '2025-03-11 00:00' then 0.00029585
            when '{{blockchain}}' = 'ethereum' and token_address = 0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8 and hour >= timestamp '2025-08-31 03:00' and hour <= timestamp '2025-08-31 04:00' then 4459.365
            when '{{blockchain}}' = 'ethereum' and token_address = 0x0b925ed163218f6662a35e0f0371ac234f9e9371 and hour >= timestamp '2025-08-29 19:00' and hour <= timestamp '2025-08-29 20:00' then 5254.7182
            else price_unit
        end as price_unit,
        case
            when '{{blockchain}}' = 'base' and token_address = 0x22af33fe49fd1fa80c7149773dde5890d3c76f3b and hour >= timestamp '2025-03-04 00:00' and hour <= timestamp '2025-03-11 00:00' then 0.00029585 / pow(10, 18)
            when '{{blockchain}}' = 'ethereum' and token_address = 0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8 and hour >= timestamp '2025-08-31 03:00' and hour <= timestamp '2025-08-31 04:00' then 4459.365 / pow(10, 18)
            when '{{blockchain}}' = 'ethereum' and token_address = 0x0b925ed163218f6662a35e0f0371ac234f9e9371 and hour >= timestamp '2025-08-29 19:00' and hour <= timestamp '2025-08-29 20:00' then 5254.7182 / pow(10,18)
            else price_atom
        end as price_atom
    from prices_pre
),

wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
            when 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            when 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WPOL
        end as native_token_address
),

-- The price of the native token is reconstructed from it chain-dependent wrapped version.
native_token_prices as (
    select -- noqa: ST06
        date_trunc('hour', minute) as hour, --noqa: RF04
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
        18 as decimals,
        avg(price) as price_unit,
        avg(price) / pow(10, 18) as price_atom
    from prices.usd
    where
        blockchain = '{{blockchain}}'
        and contract_address = (select native_token_address from wrapped_native_token)
        and minute >= cast('{{start_time}}' as timestamp) and minute < cast('{{end_time}}' as timestamp)
    group by 1, 2, 3
)

select * from prices
union all
select * from native_token_prices

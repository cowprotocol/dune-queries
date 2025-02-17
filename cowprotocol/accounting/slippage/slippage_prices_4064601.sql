-- This query computes prices for use in slippage on CoW Protocol
--
-- It gives hourly prices for all tokens transferred to or from the protocol.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{price_feed}} -- option to user either the dune_price_feed (which has been used up till now) or the median_price_feed
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

-- Fetching all additional price feeds that are synced to Dune
imported_price_feeds as (
    select --noqa: ST06
        a.source as price_source,
        date_trunc('hour', a.minute) as hour, --noqa: RF04
        a.contract_address as token_address,
        a.decimals,
        avg(a.price) as price_unit
    from "query_4252674" as a inner join token_times as tt
        on
            date_trunc('hour', a.minute) = tt.hour
            and a.contract_address = tt.token_address
            and a.blockchain = '{{blockchain}}'
    group by 1, 2, 3, 4
),

-- the Dune price feed; note that this is computed on Dune and is not part of the imported_price_feeds_raw table.
dune_price_feed as (
    select -- noqa: ST06
        'dune' as price_source,
        date_trunc('hour', a.minute) as hour, --noqa: RF04
        a.contract_address as token_address,
        a.decimals,
        avg(a.price) as price_unit
    from prices.usd as a inner join token_times as tt
        on
            date_trunc('hour', a.minute) = tt.hour
            and a.contract_address = tt.token_address
            and a.blockchain = '{{blockchain}}'
    group by 1, 2, 3, 4
),

-- we now collect together all different price feeds that we have
all_price_feeds as (
    select *
    from imported_price_feeds
    union all
    select *
    from dune_price_feed
),

-- we are now ready to define a new price feed that is the median of all price feeds defined above
-- there is an intermediate table to help with the calculation,
-- and the code for the median is based on the No.2 section
-- of this article: https://medium.com/learning-sql/how-to-calculate-median-the-right-way-in-postgresql-f7b84e9e2df7
intermediate_compute_median_table as (
    select
        hour,
        token_address,
        decimals,
        price_unit,
        row_number() over (partition by hour, token_address, decimals order by price_unit asc) as rn_asc,
        count(*) over (partition by hour, token_address, decimals) as ct
    from all_price_feeds
),

-- this is the final table generated, that uses the median of all price feeds
-- to compute a final price.
median_price_feed as (
    select
        hour,
        token_address,
        decimals,
        avg(price_unit) as price_unit
    from intermediate_compute_median_table
    where rn_asc between ct / 2.0 and ct / 2.0 + 1
    group by 1, 2, 3
),

-- We now define the precise_prices table, and there are 2 options to choose from,
-- either the median_price_feed table or the dune_price_feed
precise_prices as (
    select *
    from {{price_feed}}
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
        avg(price_unit) as price_unit
    from (
        select
            date_trunc('hour', block_time) as hour, --noqa: RF04
            buy_token_address as token_address,
            round(log(10, atoms_bought / units_bought)) as decimals,
            usd_value / units_bought as price_unit
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and buy_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        union distinct
        select
            date_trunc('hour', block_time) as hour, --noqa: RF04
            sell_token_address as token_address,
            round(log(10, atoms_sold / units_sold)) as decimals,
            usd_value / units_sold as price_unit
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and sell_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    ) as combined
    group by 1, 2, 3
),

-- The final price is the precise price if it exists and the intrinsic price otherwise.
-- If both prices are not available, the price is null.
prices as (
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
            precise.price_unit,
            intrinsic.price_unit
        ) / pow(10, coalesce(precise.decimals, intrinsic.decimals)) as price_atom
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

-- We also want to have the prices of the native token of each chain
-- so we define this intermediate table to help with that
wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
        end as native_token_address
),

-- The price of the native token is reconstructed from its chain-dependent wrapped version.
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
union distinct -- new price feeds might already entries for the native token
select * from native_token_prices

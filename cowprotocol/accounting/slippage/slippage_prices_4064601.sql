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
-- - price_usd: USD price of one unit (i.e. pow(10, decimals) atoms) of a token
-- - price_atom: USD price of one atom (i.e. 1. / pow(10, decimals) units) of a token

with token_times as (
    select
        token_address,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4021257(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
    where token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    group by 1, 2
),

precise_prices as (
    select
        token_address,
        decimals,
        date_trunc('hour', minute) as hour, --noqa: RF04
        avg(price) as price_usd,
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

intrinsic_prices as (
    select
        token_address,
        decimals,
        hour,
        avg(price_usd) as price_usd,
        avg(price_atom) as price_atom
    from (
        select
            buy_token_address as token_address,
            round(log(10, atoms_bought / units_bought)) as decimals,
            date_trunc('hour', block_time) as hour, --noqa: RF04
            usd_value / units_bought as price_usd,
            usd_value / atoms_bought as price_atom
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and buy_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        union distinct
        select
            sell_token_address as token_address,
            round(log(10, atoms_sold / units_sold)) as decimals,
            date_trunc('hour', block_time) as hour, --noqa: RF04
            usd_value / units_sold as price_usd,
            usd_value / atoms_sold as price_atom
        from cow_protocol_{{blockchain}}.trades
        where
            block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
            and sell_token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    ) as combined
    group by 1, 2, 3
),

-- -- Price Construction: https://dune.com/queries/1579091?
prices as (
    select
        tt.hour,
        tt.token_address,
        coalesce(
            precise.decimals,
            intrinsic.decimals
        ) as decimals,
        coalesce(
            precise.price_usd,
            intrinsic.price_usd
        ) as price_usd,
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

wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
        end as native_token_address
),

native_token_prices as (
    select
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
        18 as decimals,
        date_trunc('hour', minute) as hour, --noqa: RF04
        avg(price) as price_usd,
        avg(price) / pow(10, 18) as price_atom
    from prices.usd
    where
        blockchain = '{{blockchain}}'
        and contract_address = (select native_token_address from wrapped_native_token)
        and minute >= cast('{{start_time}}' as timestamp) and minute < cast('{{end_time}}' as timestamp)
    group by 3
)

select * from prices
union all
select * from native_token_prices

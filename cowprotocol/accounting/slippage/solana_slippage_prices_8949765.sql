-- Hourly USD prices for tokens moving through CoW Protocol on Solana (used for slippage)
--
-- Parameters:
--  {{start_time}} - start of the analysis window (inclusive)
--  {{end_time}}   - end of the analysis window (exclusive)
--
-- Columns:
-- - hour:          hour the price is valid for
-- - token_address: SPL mint address (base58);
--                  11111111111111111111111111111111 (System Program ID) for native SOL
-- - decimals:      token decimals, can be null
-- - price_unit:    USD price of one unit (pow(10, decimals) atoms)
-- - price_atom:    USD price of one atom (1 / pow(10, decimals) units)

with token_times as (
    select distinct
        token_address,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_8863696(start_time='{{start_time}}',end_time='{{end_time}}')"
    where token_address != '11111111111111111111111111111111'  -- native SOL priced below
),

precise_prices as (
    select
        date_trunc('hour', p.minute) as hour,
        to_base58(p.contract_address) as token_address,
        p.decimals,
        avg(p.price) as price_unit,
        avg(p.price) / pow(10, p.decimals) as price_atom
    from prices.usd as p
    where
        p.blockchain = 'solana'
        and p.minute >= cast('{{start_time}}' as timestamp)
        and p.minute <  cast('{{end_time}}' as timestamp)
        and p.contract_address in (select from_base58(token_address) from token_times)
    group by 1, 2, 3
),

-- Every requested (token, hour); price is null where the feed has none
prices as (
    select
        tt.hour,
        tt.token_address,
        pp.decimals,
        pp.price_unit,
        pp.price_atom
    from token_times as tt
    left join precise_prices as pp
        on tt.hour = pp.hour
        and tt.token_address = pp.token_address
),

-- Native SOL, priced from wrapped SOL
native_token_prices as (
    select
        date_trunc('hour', minute) as hour, --noqa: RF04
        '11111111111111111111111111111111' as token_address,
        9 as decimals,
        avg(price) as price_unit,
        avg(price) / pow(10, 9) as price_atom
    from prices.usd
    where
        blockchain = 'solana'
        and contract_address = from_base58('So11111111111111111111111111111111111111112')
        and minute >= cast('{{start_time}}' as timestamp)
        and minute <  cast('{{end_time}}' as timestamp)
    group by 1
)

select * from prices
union all
select * from native_token_prices

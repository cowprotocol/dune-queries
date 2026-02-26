-- Markout calculation for CoW Protocol trades. Compares the price of prices.usd table at the minute of the trade to the executed amounts.
-- Now this query also shows Solver Efficiency, i.e., a metric representing the execution quality before any protocol fees are applied

-- Parameters:
--  {{blockchain}} - The chain to be analysed
--  {{start_date}} - Start date of analysis
--  {{end_date}} - End date of analysis
--  {{date_granularity}} - How granular the time periods should be
--  {{min_usd_amount}} - Minimum USD amount of the trade
--  {{max_usd_amount}} - Maximum USD amount of the trade
--  {{order_source}} - Whether the trade originated directly through the UI or via integration
--  {{token_pair}} - Direction agnostic, alphabetical order (eg: USDC-WETH)
--  {{xrate_type}} - Whether the token pair has a stable or variable exchange rate
with 
prep as (
    select 
        t.block_time,
        t.tx_hash,
        usd_value,
        t.buy_token_address,
        t.sell_token_address,
        t.buy_token,
        t.sell_token,
        units_bought,
        units_bought + coalesce(if(rod.protocol_fee_token=t.buy_token_address, rod.protocol_fee/(t.atoms_bought/t.units_bought), 0),0) as units_bought_eff,
        units_sold,
        units_sold - coalesce(if(rod.protocol_fee_token=t.sell_token_address, rod.protocol_fee/(t.atoms_sold/t.units_sold), 0),0) as units_sold_eff,
        buy_price,
        sell_price,
        t.token_pair,
        if(replace(lower(ad.app_code),' ','') = 'cowswap', 'UI', 'Integrations') as order_source,
        if(st.ref_date is not null, 'stable', 'variable') as xrate_type
    from cow_protocol_{{blockchain}}.trades as t 
    inner join "query_4364122(blockchain='{{blockchain}}')" as rod
        on t.tx_hash = rod.tx_hash 
        and t.order_uid = rod.order_uid
    left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as ad
        on t.app_data = ad.app_hash
    left join "query_5719467(blockchain='{{blockchain}}', start_date='{{start_date}}', end_date='{{end_date}}')" as st  
        on st.sell_token_address = t.sell_token_address
        and st.buy_token_address = t.buy_token_address
        and date(st.ref_date) = t.block_date 
    where
        t.block_time >= timestamp '{{start_date}}'
        and t.block_time < timestamp '{{end_date}}'
        and t.usd_value between {{min_usd_amount}} and {{max_usd_amount}} 
        and if(upper('{{order_source}}')='ALL', true, if(replace(lower(ad.app_code),' ','') = 'cowswap', 'UI', 'Integrations') = '{{order_source}}')
        and if(upper('{{token_pair}}')='ALL', true, upper(t.token_pair) = upper('{{token_pair}}'))
        and if(upper('{{xrate_type}}')='ALL', true, if(st.ref_date is not null, 'stable', 'variable') = '{{xrate_type}}')
)
, markouts_per_trade as (
    select
        *,
        (units_bought / units_sold) * (buy_price / sell_price) - 1 as markout,
        (units_bought_eff / units_sold_eff) * (buy_price / sell_price)  - 1 as solver_efficiency
    from prep
    where 
        sell_price > 0 
        and buy_price > 0
)
select
    date_trunc('{{date_granularity}}', block_time) as date,
    1e4 * approx_percentile(markout, 0.1) as markout_p10_bps,
    1e4 * approx_percentile(markout, 0.5) as markout_p50_bps,
    1e4 * approx_percentile(markout, 0.9) as markout_p90_bps,
    1e4 * approx_percentile(solver_efficiency, 0.1) as solver_efficiency_p10_bps,
    1e4 * approx_percentile(solver_efficiency, 0.5) as solver_efficiency_p50_bps,
    1e4 * approx_percentile(solver_efficiency, 0.9) as solver_efficiency_p90_bps
from markouts_per_trade
group by 1

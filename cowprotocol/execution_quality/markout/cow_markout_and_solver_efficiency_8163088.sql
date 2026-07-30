-- Execution Quality Metrics
-- Markout: compares execution prices vs price feed references at minute level.
-- Solver Efficiency: represents the execution quality before any protocol fees are applied (same as markouts but gross of fees)

with 
params as (
    select 
        split('{{blockchain}}', ',') as blockchains
        , '{{date_granularity}}' as date_granularity
        , '{{trade_source}}' as trade_source
        , '{{tokens_correlation}}' as tokens_correlation
        , {{min_usd_amount}} as min_usd_amount
        , {{max_usd_amount}} as max_usd_amount
        , timestamp '{{start_time}}' as start_time
        , timestamp '{{end_time}}' as end_time
        , (select period_completion_ratio['{{date_granularity}}'] from "query_5633784") as period_completion_ratio
)
, prep as (
    select 
        t.block_time,
        t.tx_hash,
        t.usd_value,
        t.buy_token_address,
        t.sell_token_address,
        t.buy_token,
        t.sell_token,
        t.units_bought,
        t.units_sold,
        t.units_bought + coalesce(if(t.order_type='SELL', rod.protocol_fee/(t.atoms_bought/t.units_bought), 0),0) as units_bought_eff,
        t.units_sold - coalesce(if(t.order_type='BUY', rod.protocol_fee/(t.atoms_sold/t.units_sold), 0),0) as units_sold_eff,
        t.buy_price,
        t.sell_price,
        t.trade_source,
        t.token_pair,
        t.tokens_correlation
    from dune.cowprotocol.fct_trades as t
    left join dune.cowprotocol.order_data as rod
        on t.order_uid=rod.order_uid
        and t.tx_hash=rod.tx_hash
        and t.blockchain=rod.blockchain
    cross join params as p
    where
        t.block_time >= p.start_time
        and t.block_time < p.end_time
        and t.usd_value between p.min_usd_amount and p.max_usd_amount 
        and if(p.trade_source='ALL', true, t.trade_source = p.trade_source)
        and if(p.tokens_correlation='ALL', true, t.tokens_correlation = p.tokens_correlation)
        and if(array_position(p.blockchains, 'All Supported Chains') > 0, true, array_position(p.blockchains, t.blockchain) > 0)
        and sell_price > 0 
        and buy_price > 0

)
, markouts_per_trade as (
    select
        *,
        (units_bought / units_sold) * (buy_price / sell_price) - 1 as markout,
        (units_bought_eff / units_sold_eff) * (buy_price / sell_price)  - 1 as solver_efficiency
    from prep
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

/*
Margin = Protocol Fees + CoW's cut of Partner Fee - Quote Rewards - Solver Rewards*
*As per our auction mechanism, solving rewards are not attributed on a trade basis but rather on a solver-auction basis. 
Our approach then is to weigh the rewards by the volume from each trade within that set
Per-trade Solver rewards are an estimation and will not perfectly match the overall rewards provided by CoW
*/
with
native_prices as (
    select timestamp, price 
    from prices.hour
    where timestamp >= timestamp '{{start_time}}'
        and timestamp < timestamp '{{end_time}}'
        and blockchain = '{{blockchain}}'
        and contract_address IN (select token_address from dune.blockchains where name = '{{blockchain}}')
)
, fees_per_trade as (
    select 
        t.block_time,
        '{{blockchain}}' as blockchain,
        t.tx_hash,
        t.order_uid,
        t.usd_value,        
        ad.app_code,
        t.token_pair,
        rod.solver,
        coalesce(r.protocol_fee_revenue_native,0) as protocol_fee,
        coalesce(r.partner_fee_cow_revenue_native,0) as partner_fee_cow_share,
        coalesce(r.protocol_fee_revenue_native,0) + coalesce(r.partner_fee_cow_revenue_native,0) as revenue
    from cow_protocol_{{blockchain}}.trades as t
    left join (select distinct * from "query_4364122(blockchain='{{blockchain}}')") as rod
        on t.tx_hash = rod.tx_hash
        and t.order_uid = rod.order_uid
    left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as ad   
        on ad.app_hash = t.app_data
    left join dune.cowprotocol.result_fees_revenue_per_order as r 
        on t.tx_hash = r.tx_hash
        and t.order_uid = r.order_uid
    where 
        t.block_time >= timestamp '{{start_time}}'
        and t.block_time < timestamp '{{end_time}}'
)
, prep_rewards as (
-- solver rewards are done on a per auction-solver basis (an auction may have multiple batches from the same solver)
    select 
          t.block_time
        , t.tx_hash
        , t.order_uid
        , rod.auction_id
        , rod.solver
        , rod.quote_solver
        
        , rbd.capped_payment/1e18 as reward_auction_solver
        , sum(t.usd_value) over (partition by rod.auction_id, rod.solver) as volume_auction_solver
        -- if usd value of trade is missing then attribute the whole auction reward to that trade - may overestimate rewards
        , if(t.usd_value != 0 
            , rbd.capped_payment/1e18 * t.usd_value / sum(t.usd_value) over (partition by rod.auction_id, rod.solver)
            , rbd.capped_payment/1e18
        ) as trade_solver_reward
        
    from (select distinct * from "query_4364122(blockchain='{{blockchain}}')") as rod
    join cow_protocol_{{blockchain}}.trades as t
        on rod.order_uid = t.order_uid
        and rod.tx_hash = t.tx_hash
    left join (select distinct * from "query_4351957(blockchain='{{blockchain}}')") as rbd
        on rbd.tx_hash = t.tx_hash
    where
        t.block_time >= timestamp '{{start_time}}'
        and t.block_time < timestamp '{{end_time}}'
)
, solving_rewards as (
    select 
        prep.* 
        , coalesce(if(sf.service_fee, 0.85*trade_solver_reward, trade_solver_reward), 0) as solver_reward
    from prep_rewards as prep 
    -- service fee adjustment
    left join "query_4298142(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')" as sf 
        on prep.solver = sf.solver
)
--select * from solving_rewards order by volume desc
, quote_cap_mapping as (
    select *
    from 
        (values
            -- Pre CIP 72
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0006, 6, 'ethereum'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.15,   6, 'gnosis'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0002, 6, 'base'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0002, 6, 'arbitrum'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0005, 6, 'avalanche_c'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.5,    6, 'polygon'),
            -- Post CIP 72
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.0007,  6, 'ethereum'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.15,    6, 'gnosis'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.00024, 6, 'base'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.00024, 6, 'arbitrum'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.0006,  6, 'avalanche_c'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.6,     6, 'polygon')
    
        ) as t(from_ts, until_ts, quote_cap_native, quote_cap_cow, blockchain)
)
, conv_native_to_cow as (
    select
        end_time as date,
        native_token_price / nullif(cow_price, 0) as native_to_cow_rate
    from dune.cowprotocol.result_accounting_cow_and_native_prices_per_chain
    where
        blockchain = '{{blockchain}}'
        and end_time >= timestamp '{{start_time}}'
        and end_time <= date_add('week', 1, date_trunc('week',timestamp '{{end_time}}'))
)
, quote_rewards as (
    select        
        prep.* 
        , coalesce(if(quote_solver is not null, least(cap.quote_cap_cow, cap.quote_cap_native * p.native_to_cow_rate) / p.native_to_cow_rate, 0), 0) as quote_reward
    from prep_rewards as prep 
    left join quote_cap_mapping as cap
        on prep.block_time > cap.from_ts
        and prep.block_time <= cap.until_ts
        and cap.blockchain = '{{blockchain}}'
    left join conv_native_to_cow as p 
        on date_trunc('week', prep.block_time - interval '1' day) + interval '8' day = p.date    
)
select 
      fees.blockchain
    , fees.block_time 
    , fees.app_code
    , fees.token_pair    
    , fees.solver
    , fees.usd_value
    , fees.usd_value / np.price as native_value
    , fees.revenue
    , fees.protocol_fee
    , fees.partner_fee_cow_share
    , qr.quote_reward as quote_reward
    , sr.solver_reward as solver_reward    
    , revenue - quote_reward - solver_reward as margin
    , (revenue - quote_reward - solver_reward) / revenue as margin_pct
    , 1e4*(revenue - quote_reward - solver_reward) / (fees.usd_value / np.price) as margin_per_vol_bps
    , fees.tx_hash
    , fees.order_uid   
from fees_per_trade as fees 
left join quote_rewards as qr
    on fees.tx_hash = qr.tx_hash
    and fees.order_uid = qr.order_uid
left join solving_rewards as sr
    on fees.tx_hash = sr.tx_hash
    and fees.order_uid = sr.order_uid
left join native_prices as np 
    on date_trunc('hour', fees.block_time) = np.timestamp
where
    fees.block_time < current_date -- remove misleading data, as some tables refresh daily
order by usd_value desc



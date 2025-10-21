-- This query is the basis for experiments with different rewards mechanisms
--
-- It is under version control in https://github.com/cowprotocol/dune-queries
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{scaling}} - fraction of protocol fees used to cap rewards
--  {{price_improvement_fee}} - fraction of price improvement charged as fee
--  {{volume_fee_bps}} - fraction of volume charged as fee
--  {{fixed_fee}} - additional fixed fee per trade
--
-- The columns of the result are
-- - time: time of the auction (deadline)
-- - auction_id: id of the auction
-- - solver: winning solver in that auction
-- - solver_name: dune name of the solver
-- - protocol_fee: sum of protocol fees charged by a solver, in native token
-- - volume: sum of volume of trades, in native token
-- - new_protocol_fee: protocol fee when charging price improvement, volume and fixed fee
-- - uncapped_reward: uncapped second price reward for the solver in that auction
-- - reward: current capped reward using caps per chain
-- - new_reward: reward based on capping from above by a fraction of new_protocol_fee, the original cap from below applies
-- - profit: protocol profit as difference of protocol fee and reward
-- - new_profit: same as profit but for new reward and new protocol fee
-- - reward_missed: amount a solver could have gotten from taking a cut instead of getting a capped reward;
--     this is a measure of how much solvers can gain from acting strategically with their bidding
-- - new_reward_missed: same as reward_missed but for new reward


with wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
            when 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            when 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WPOL
            when 'lens' then 0x6bdc36e20d267ff0dd6097799f82e78907105e2f -- WGHO
        end as native_token_address
),

batch_data as (
    select
        b.time,
        rbd.auction_id,
        rbd.solver,
        s.name as solver_name,
        rbd.uncapped_payment_native_token as uncapped_reward,
        rbd.capped_payment as reward,
        count(*) as number_of_trades,
        sum((rod.protocol_fee - coalesce(rod.partner_fee, 0)) * rod.protocol_fee_native_price) as protocol_fee, -- this is the actual revenue of the protocol
        sum(case when t.order_type = 'SELL' then atoms_bought * rod.protocol_fee_native_price else atoms_sold * rod.protocol_fee_native_price end) as volume,
        bool_and(case when t.order_type = 'SELL' then (rod.protocol_fee_native_price * atoms_bought * p.price / 1e18) / coalesce(t.buy_price * units_bought, usd_value) < 2 else (rod.protocol_fee_native_price  * atoms_sold * p.price / 1e18) / coalesce(t.sell_price * units_sold, usd_value) < 2 end) as native_prices_are_accurate
    from "query_4351957(blockchain='{{blockchain}}')" as rbd
    join "query_4364122(blockchain='{{blockchain}}')" as rod
        on rbd.auction_id = rod.auction_id and rbd.solver = rod.solver and rbd.tx_hash = rod.tx_hash
    join cow_protocol_{{blockchain}}.trades as t
        on rod.order_uid = t.order_uid and rod.tx_hash = t.tx_hash
    join {{blockchain}}.blocks as b
        on rbd.block_deadline = b.number
    join cow_protocol_{{blockchain}}.solvers as s
        on rbd.solver = s.address
    join prices.day as p
        on date_trunc('day', b.time) = p.timestamp
        and p.contract_address = (select * from wrapped_native_token)
        and p.blockchain = '{{blockchain}}'
    where b.time >= (timestamp '{{start_time}}') and b.time < (timestamp '{{end_time}}')
    group by 1, 2, 3, 4, 5, 6
),

caps as (
    select
        max(reward) as upper_cap,
        min(reward) as lower_cap
    from batch_data
),

rewards_per_auction as (
    select
        time,
        auction_id,
        solver,
        solver_name,
        protocol_fee,
        volume,
        if('{{price_improvement_fee}}'='on', protocol_fee, 0) + volume * {{volume_fee_bps}} / 1e4 + {{fixed_fee}} * 1e18 * number_of_trades as new_protocol_fee,
        uncapped_reward,
        reward
    from batch_data
    where
        native_prices_are_accurate -- this filters out cases where native prices are not accurate
        and uncapped_reward < volume -- this filters out cases where native prices were corrected but rewards are too large
),

new_rewards_per_auction as (
    select
        *,
        least({{scaling}} * new_protocol_fee, greatest((select lower_cap from caps), uncapped_reward)) as new_reward
    from rewards_per_auction
)

select
    *,
    protocol_fee - reward as profit,
    new_protocol_fee - new_reward as new_profit,
    if(uncapped_reward >=0, uncapped_reward - reward, 0) as reward_missed,
    if(uncapped_reward >=0, uncapped_reward - new_reward, 0) as new_reward_missed
from new_rewards_per_auction

-- This query is the basis for experiments with capping rewards by a fraction of protocol fees
--
-- It is under version control in https://github.com/cowprotocol/dune-queries
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{volume_fee_bps_stable}} - fraction of volume charged as fee on correlated tokens
--  {{volume_fee_bps_variable}} - fraction of volume charged as fee on uncorrelated tokens
--
-- The columns of the result are
-- - time: time of the auction (deadline)
-- - auction_id: id of the auction
-- - solver: winning solver in that auction
-- - xrate_type: either 'stable' for trades between highly correlated tokens, 'variable' for trades between uncorrelated tokens,
--     None if a classification was not possible
-- - protocol_fee: sum of protocol fees (excluding fees charged by partners) charged by a solver, in native token
-- - volume: sum of volume of trades, in native token
-- - new_protocol_fee: new protocol fee when changing the volume fee for some class of orders
-- - uncapped_reward: uncapped second price reward for the solver in that auction
-- - reward: current reward
-- - new_reward: reward based on capping from above by new_protocol_fee, the original cap from below applies
-- - old_reward: reward based on static caps
-- - profit: protocol profit as difference of protocol fee and reward
-- - new_profit: same as profit but for new reward and new protocol fee
-- - reward_missed: amount a solver could have gotten from taking a cut instead of getting a capped reward;
--     this is a measure of how much solvers can gain from acting strategically with their bidding
-- - new_reward_missed: same as reward_missed but for new reward
-- - old_reward_missed: same as reward_missed but for old reward


with wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            -- when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
            -- when 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            -- when 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WPOL
            -- when 'lens' then 0x6bdc36e20d267ff0dd6097799f82e78907105e2f -- WGHO
        end as native_token_address
),

reward_caps as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 12000000000000000 -- 0.012 ETH
            when 'arbitrum' then 12000000000000000 -- 0.012 ETH
            when 'base' then 12000000000000000 -- 0.012 ETH
        end as upper_cap,
        case '{{blockchain}}'
            when 'ethereum' then 10000000000000000 -- 0.01 ETH
            when 'arbitrum' then 10000000000000000 -- 0.01 ETH
            when 'base' then 10000000000000000 -- 0.01 ETH
        end as lower_cap
),

aggregated_batch_data as (
    select
        b.time,
        rbd.auction_id,
        rbd.solver,
        rbd.uncapped_payment_native_token as uncapped_reward,
        coalesce(sum((rod.protocol_fee - coalesce(rod.partner_fee, 0)) * rod.protocol_fee_native_price), 0) as protocol_fee, -- this is the actual revenue of the protocol
        coalesce(sum(case when t.order_type = 'SELL' then t.atoms_bought * rod.protocol_fee_native_price else t.atoms_sold * rod.protocol_fee_native_price end), 0) as volume,
        bool_and(
            case
                when t.order_type = 'SELL' then abs((rod.protocol_fee_native_price * t.atoms_bought * p.price / 1e18) / coalesce(t.buy_value_usd, t.usd_value) - 1) < 0.2
                else abs((rod.protocol_fee_native_price  * t.atoms_sold * p.price / 1e18) / coalesce(t.sell_value_usd, t.usd_value) - 1) < 0.2
            end
        ) as native_prices_are_accurate,
        bool_or(t.tx_hash is not null) as at_least_partial_success, -- this implies that there is data on trades which makes checking native prices meaningful
        if(bool_and(st.ref_date is not null), 'stable', if(bool_and(t.sell_token_address is not null and st.ref_date is null), 'variable')) as xrate_type
    from "query_4351957(blockchain='{{blockchain}}')" as rbd
    left join "query_4364122(blockchain='{{blockchain}}')" as rod
        on rbd.auction_id = rod.auction_id and rbd.solver = rod.solver and rbd.tx_hash = rod.tx_hash
    left join cow_protocol_{{blockchain}}.trades as t
        on rod.order_uid = t.order_uid and rod.tx_hash = t.tx_hash
    left join {{blockchain}}.blocks as b
        on rbd.block_deadline = b.number
    left join prices.day as p
        on date_trunc('day', b.time) = p.timestamp
        and p.contract_address = (select * from wrapped_native_token)
        and p.blockchain = '{{blockchain}}'
    left join "query_5719467(blockchain='{{blockchain}}', start_date='{{start_time}}', end_date='{{end_time}}')" as st
        on t.sell_token_address = st.sell_token_address
        and t.buy_token_address = st.buy_token_address
        and t.block_date = date(st.ref_date)
    where b.time >= (timestamp '{{start_time}}') and b.time < (timestamp '{{end_time}}')
    group by 1, 2, 3, 4
),

rewards_per_auction as (
    select
        time,
        auction_id,
        solver,
        xrate_type,
        protocol_fee,
        volume,
        protocol_fee + volume * (if(xrate_type = 'stable', {{volume_fee_bps_stable}}, {{volume_fee_bps_variable}}) - 2.0) / 1e4 as new_protocol_fee,
        uncapped_reward
    from aggregated_batch_data
    where
        (
            native_prices_are_accurate -- this filters out cases where native prices are not accurate
            and abs(uncapped_reward) < volume -- this filters out cases where native prices were corrected but rewards are too large
        )
        or not at_least_partial_success -- this makes sure the filtering is only applied if some onchain data exists
),

new_rewards_per_auction as (
    select
        *,
        least(protocol_fee, greatest(uncapped_reward, -(select lower_cap from reward_caps))) as reward,
        least(new_protocol_fee, greatest(uncapped_reward, -(select lower_cap from reward_caps))) as new_reward,
        least((select upper_cap from reward_caps), greatest(uncapped_reward, -(select lower_cap from reward_caps))) as old_reward
    from rewards_per_auction
)

select
    *,
    protocol_fee - reward as profit,
    new_protocol_fee - new_reward as new_profit,
    if(uncapped_reward >=0, uncapped_reward - reward, 0) as reward_missed,
    if(uncapped_reward >=0, uncapped_reward - new_reward, 0) as new_reward_missed,
    if(uncapped_reward >=0, uncapped_reward - old_reward, 0) as old_reward_missed
from new_rewards_per_auction

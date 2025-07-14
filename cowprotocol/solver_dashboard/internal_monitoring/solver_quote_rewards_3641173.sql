--Error: Query is too long for wand tools. 
--Try shortening query to try again. 
-- BATCH REWARDS
with
batch_rewards as (
    select
        bs.winning_solver as solver,
        bs.participating_solvers,
        date_add('day', 1, date_trunc('week', date_add('day', -1, eb.time))) as week_start,
        case
            when eb.time >= cast('2024-03-19 12:00:00' as timestamp) then reward -- switch to CIP-38
            else bs.reward - bs.execution_cost
        end as reward,
        case
            when eb.time >= cast('2024-07-23 00:00:00' as timestamp) then 'CIP-48' -- switch to CIP-48
            when eb.time >= cast('2024-03-19 12:00:00' as timestamp) then 'CIP-38' -- switch to CIP-38
            when eb.time >= cast('2024-02-06 00:00:00' as timestamp) then 'CIP-36' -- switch to CIP-36
            else 'CIP-27'
        end as cip
    from query_2777544 as bs
    join ethereum.blocks as eb --noqa: AM05
        on bs.block_deadline = eb.number
    where
        eb.time >= cast('2023-07-18 00:00:00' as timestamp) -- start of analysis
),

participation_data as (
    select
        week_start,
        participant
    from batch_rewards as br
    cross join unnest(br.participating_solvers) as t (participant) --noqa: AL05
),

participation_counts as (
    select
        week_start,
        participant as solver,
        count(*) as num_participating_batches
    from participation_data
    group by
        week_start, participant
),

batch_rewards_aggregate as (
    select
        br.week_start,
        br.solver,
        sum(reward) as performance_reward,
        max(num_participating_batches) as num_participating_batches, -- there is only one value and the maximum selects it
        max(cip) as cip -- there is only one value and the maximum selects it
    from
        batch_rewards as br
    join participation_counts as pc --noqa: AM05
        on
            br.week_start = pc.week_start
            and br.solver = pc.solver
    group by
        br.week_start, br.solver
),

week_data as (
    select
        week_start,
        max(cip) as cip, -- there is only one value and the maximum selects it
        sum(performance_reward) as performance_reward,
        sum(num_participating_batches) as num_participating_batches
    from
        batch_rewards_aggregate
    group by
        week_start
),

week_data_with_caps as (
    select
        *,
        case
            when cip = 'CIP-48' then 250000 -- switch to CIP-48
            when cip = 'CIP-38' then 250000 -- switch to CIP-38
            when cip = 'CIP-36' then 250000 -- switch to CIP-36
            else 306307-- 'CIP-27'
        end as reward_budget_cow,
        case
            when cip = 'CIP-48' then 0 -- switch to CIP-48
            when cip = 'CIP-38' then 6 -- switch to CIP-38
            when cip = 'CIP-36' then 6 -- switch to CIP-36
            else 1000 -- actually no cap in CIP-27
        end as consistency_cap_eth,
        case
            when cip = 'CIP-48' then 6 -- switch to CIP-38
            when cip = 'CIP-38' then 6 -- switch to CIP-38
            when cip = 'CIP-36' then 6 -- switch to CIP-36
            else 9 -- 'CIP-27'
        end as quote_reward_cow,
        case
            when cip = 'CIP-48' then 0.0006 -- switch to CIP-38
            when cip = 'CIP-38' then 0.0006 -- switch to CIP-38
            when cip = 'CIP-36' then 0.0006 -- switch to CIP-36
            else 1000 -- actually no cap in CIP-27
        end as quote_cap_eth
    from week_data
),

conversion_prices as (
    select
        week_start,
        (
            select avg(price)
            from
                prices.minute
            where
                blockchain = 'ethereum'
                and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
                and date(timestamp) = date_add('day', 6, week_start)
        ) as cow_price,
        (
            select avg(price)
            from
                prices.minute
            where
                blockchain = 'ethereum'
                and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                and date(timestamp) = date_add('day', 6, week_start)
        ) as eth_price
    from
        week_data
),

-- BEGIN QUOTE REWARDS
order_quotes as (
    select
        quote_solver as solver,
        date_add('day', 1, date_trunc('week', date_add('day', -1, time))) as week_start
    from
        query_3373259
    join ethereum.blocks as e on query_3373259.block_number = e.number --noqa: AM05
    where
        time >= cast('2023-07-18 00:00:00' as timestamp) -- start of analysis
        and quote_solver != 0x0000000000000000000000000000000000000000
),

quote_numbers as (
    select
        week_start,
        solver,
        count(*) as num_quotes
    from
        order_quotes
    group by
        week_start, solver
),

results as (
    select
        batch_rewards_aggregate.week_start,
        batch_rewards_aggregate.solver,
        concat(cow_protocol_ethereum.solvers.environment, '-', cow_protocol_ethereum.solvers.name) as solver_name,
        eth_price / cow_price * batch_rewards_aggregate.performance_reward / pow(10, 18) as performance_reward,
        greatest(
            0,
            least(
                eth_price / cow_price * consistency_cap_eth,
                reward_budget_cow - eth_price / cow_price * week_data_with_caps.performance_reward / pow(10, 18)
            )
        ) * batch_rewards_aggregate.num_participating_batches / week_data_with_caps.num_participating_batches as consistency_reward,
        least(quote_reward_cow, quote_cap_eth * eth_price / cow_price) * num_quotes as quote_reward
    from
        batch_rewards_aggregate --noqa: ST09
    left outer join quote_numbers
        on
            batch_rewards_aggregate.week_start = quote_numbers.week_start
            and batch_rewards_aggregate.solver = quote_numbers.solver
    left outer join week_data_with_caps
        on batch_rewards_aggregate.week_start = week_data_with_caps.week_start
    left outer join conversion_prices
        on batch_rewards_aggregate.week_start = conversion_prices.week_start
    left outer join cow_protocol_ethereum.solvers
        on cow_protocol_ethereum.solvers.address = batch_rewards_aggregate.solver
)

select *
from
    results
order by
    week_start, solver

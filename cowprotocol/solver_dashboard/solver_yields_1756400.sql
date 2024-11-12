with
solver_first_rewarded_batch as (
    select
        solver_address as solver,
        concat(environment, '-', name) as solver_name,
        count(*) as num_batches,
        sum(num_trades) as num_trades,
        min(block_time) as first_batch,
        max(block_time) as latest_batch,
        -- datediff(now()::date, min(block_time)::date) as days_since_first_batch
        -- DuneSQL
        date_diff('day', min(block_time), now()) as days_since_first_batch
    from cow_protocol_ethereum.batches
    inner join cow_protocol_ethereum.solvers
        on address = solver_address
    where
        block_time > cast('2022-03-01' as timestamp)
        and environment in ('barn', 'prod')
    group by solver_address, environment, name
),

unmodified_rewards as (
    select
        from_hex(solver) as solver,
        sum(data.amount) as cow_reward
    from cowswap.raw_order_rewards
    group by solver
)


select
    s.solver,
    solver_name,
    num_batches,
    num_trades,
    first_batch,
    latest_batch,
    days_since_first_batch,
    coalesce(cow_reward, 0) as approx_cow_reward
from solver_first_rewarded_batch as s
left outer join unmodified_rewards as r
    on s.solver = r.solver
order by latest_batch desc

-- select solver,
--     solver_name,
--     first_batch,
--     cow_amount,
--     total_value_earned,
--     total_value_earned / days_since_first_batch as average_daily_income, 
--     365.0 * total_value_earned / days_since_first_batch as estimated_annual_income
-- from solver_totals
-- order by average_daily_income desc

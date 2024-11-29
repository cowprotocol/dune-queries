-- This query computes how much cost the sovlers covered per environment
-- It takes into account the fee and the gas cost of the transactions
-- The query is not up to date with the new reporting of fees

with
fee_and_cost_per_batch as (
    select
        block_time,
        tx_hash,
        fee_value as fee,
        tx_cost_usd as gas_cost,
        environment as solver_env,
        name as solver_name
    from
        cow_protocol_ethereum.batches
    inner join cow_protocol_ethereum.solvers on address = solver_address
    where
        block_time > now() - interval '2' month
),

failed_settlements as (
    select --noqa: ST06
        block_time,
        hash as tx_hash,
        0 as fee,
        (gas_used * gas_price * p.price) / pow(10, 18) as gas_cost,
        environment as solver_env,
        name as solver_name
    from
        ethereum.transactions
    inner join prices.minute as p on p.minute = date_trunc('minute', block_time) --noqa: LT02
        and blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    inner join cow_protocol_ethereum.solvers on "from" = address --noqa: LT02
        and position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
        and success = false
    where
        block_time > now() - interval '2' month
),

results as (
    select
        solver_env,
        date(block_time) as day, -- noqa
        sum(fee) / sum(gas_cost) as cost_coverage
    from
        (
            select *
            from
                fee_and_cost_per_batch
            union distinct
            select *
            from
                failed_settlements
        )
    group by date(block_time), solver_env
)

select *
from
    results
where
    solver_env in ('prod', 'barn')
order by
    cost_coverage desc

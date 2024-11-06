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
            join cow_protocol_ethereum.solvers on address = solver_address
        where
            block_time > NOW () - INTERVAL '2' MONTH
    ),
    failed_settlements as (
        select
            block_time,
            hash as tx_hash,
            0 as fee,
            (gas_used * gas_price * p.price) / pow (10, 18) as gas_cost,
            environment as solver_env,
            name as solver_name
        from
            ethereum.transactions
            join prices.usd as p on p.minute = date_trunc ('minute', block_time)
            and blockchain = 'ethereum'
            and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            join cow_protocol_ethereum.solvers on "from" = address
            and position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
            and success = false
        where
            block_time > NOW () - INTERVAL '2' MONTH
    ),
    results as (
        select
            date (block_time) as day,
            solver_env,
            sum(fee) / sum(gas_cost) cost_coverage
        from
            (
                select
                    *
                from
                    fee_and_cost_per_batch
                union
                select
                    *
                from
                    failed_settlements
            ) as _
        group by
            date (block_time),
            solver_env
    )
select
    *
from
    results
where
    solver_env in ('prod', 'barn')
order by
    cost_coverage desc
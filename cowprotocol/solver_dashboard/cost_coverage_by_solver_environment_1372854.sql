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
            block_time > NOW() - interval '2' month
    ),
    failed_settlements as (
        select
            block_time,
            hash as tx_hash,
            0 as fee,
            (gas_used * gas_price * p.price) / POW(10, 18) as gas_cost,
            environment as solver_env,
            name as solver_name
        from
            ethereum.transactions
            inner join prices.usd as p on p.minute = DATE_TRUNC('minute', block_time)
            and blockchain = 'ethereum'
            and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            inner join cow_protocol_ethereum.solvers on "from" = address
            and POSITION('0x13d79a0b' in CAST(data as varchar)) > 0 --! settle method ID
            and success = false
        where
            block_time > NOW() - interval '2' month
    ),
    results as (
        select
            solver_env,
            DATE(block_time) as day,
            SUM(fee) / SUM(gas_cost) as cost_coverage
        from
            (
                select *
                from
                    fee_and_cost_per_batch
                union distinct
                select *
                from
                    failed_settlements
            ) as _
        group by
            date (block_time),
            solver_env
    )
select *
from
    results
where
    solver_env in ('prod', 'barn')
order by
    cost_coverage desc

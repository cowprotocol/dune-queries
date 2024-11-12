with 
fee_and_cost_per_batch as (
    select 
        block_time,
        tx_hash,
        fee_value as fee,
        tx_cost_usd as gas_cost,
        environment as solver_env,
        name as solver_name
    from cow_protocol_ethereum.batches
        JOIN cow_protocol_ethereum.solvers 
            ON address = solver_address
    where tx_hash not in (
        0x84d57d1d57e01dd34091c763765ddda6ff713ad67840f39735f0bf0cced11f02,
        0x918eacff2b6c1fdbb14920473974dd471301f9f305c010baa3085b9ed59c33a6,
        0x4ef7702110e9ae27615970e92226462083c6a3811468890e0bd94c48655b5752
    )
),

failed_settlements as (
    select 
        block_time,
        hash as tx_hash,
        0 as fee,
        (gas_used * gas_price * p.price) / pow(10, 18) as gas_cost,
        environment as solver_env,
        name as solver_name
    from ethereum.transactions
        join prices.usd as p 
            on p.minute = date_trunc('minute', block_time)
            and blockchain = 'ethereum'
            and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        join cow_protocol_ethereum.solvers 
            on "from" = address
            and position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
            and success = false
),

results as (
    select 
        date_trunc('hour', block_time) as hour,
        solver_env,
        solver_name,
        sum(fee) / sum(gas_cost) cost_coverage
    from (
        select * from fee_and_cost_per_batch
        union
        select * from failed_settlements
    ) as _
    group by date_trunc('hour', block_time), solver_env, solver_name
),

production_coverage as (
    select 
        hour,
        solver_name,
        cost_coverage
    from results
    where solver_env = '{{SolverEnv}}'
),

result_average as (
    select 
        hour,
        'average' as solver_name,
        avg(cost_coverage) as cost_coverage
    from production_coverage
    group by hour
)

select *
from (
        select *
        from production_coverage
        union
        select *
        from result_average
    ) as _
where hour > NOW() - INTERVAL '{{Interval Length}}' {{Units}}
order by cost_coverage desc

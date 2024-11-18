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
    inner join cow_protocol_ethereum.solvers on address = solver_address
    where tx_hash not in (
        0x84d57d1d57e01dd34091c763765ddda6ff713ad67840f39735f0bf0cced11f02,
        0x918eacff2b6c1fdbb14920473974dd471301f9f305c010baa3085b9ed59c33a6,
        0x4ef7702110e9ae27615970e92226462083c6a3811468890e0bd94c48655b5752
    )
),

failed_settlements as (
    select --noqa: ST06
        block_time,
        hash as tx_hash,
        0 as fee,
        (gas_used * gas_price * p.price) / pow(10, 18) as gas_cost,
        environment as solver_env,
        name as solver_name
    from ethereum.transactions
    inner join prices.usd as p on p.minute = date_trunc('minute', block_time) --noqa: LT02
        and blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    inner join cow_protocol_ethereum.solvers on "from" = address --noqa: LT02
        and position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
        and success = false
),

results as (
    select --noqa: ST06
        date_trunc('hour', block_time) as hour, --noqa: RF04
        solver_env,
        solver_name,
        sum(fee) / sum(gas_cost) as cost_coverage
    from (
        select * from fee_and_cost_per_batch
        union all
        select * from failed_settlements
    )
    group by date_trunc('hour', block_time), solver_env, solver_name
),

production_coverage as (
    select
        hour,
        solver_name,
        cost_coverage
    from results
    where solver_env = '{{solver_env}}'
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
    union all
    select *
    from result_average
)
where hour > now() - interval '{{interval_length}}' {{units}}
order by cost_coverage desc

WITH 
fee_and_cost_per_batch as (
    SELECT 
        block_time,
        tx_hash,
        fee_value as fee,
        tx_cost_usd as gas_cost,
        environment as solver_env,
        name as solver_name
    FROM cow_protocol_ethereum.batches
        JOIN cow_protocol_ethereum.solvers 
            ON address = solver_address
    WHERE tx_hash not in (
        0x84d57d1d57e01dd34091c763765ddda6ff713ad67840f39735f0bf0cced11f02,
        0x918eacff2b6c1fdbb14920473974dd471301f9f305c010baa3085b9ed59c33a6,
        0x4ef7702110e9ae27615970e92226462083c6a3811468890e0bd94c48655b5752
    )
),
failed_settlements as (
    SELECT 
        block_time,
        hash as tx_hash,
        0 as fee,
        (gas_used * gas_price * p.price) / pow(10, 18) as gas_cost,
        environment as solver_env,
        name as solver_name
    FROM ethereum.transactions
        JOIN prices.usd as p 
            ON p.minute = date_trunc('minute', block_time)
            and blockchain = 'ethereum'
            and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        JOIN cow_protocol_ethereum.solvers 
            ON "from" = address
            AND position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
            AND success = false
),
results as (
    SELECT 
        date_trunc('hour', block_time) as hour,
        solver_env,
        solver_name,
        sum(fee) / sum(gas_cost) cost_coverage
    FROM (
        SELECT * FROM fee_and_cost_per_batch
        UNION
        SELECT * FROM failed_settlements
    ) as _
    GROUP by date_trunc('hour', block_time), solver_env, solver_name
),
production_coverage as (
    SELECT 
        hour,
        solver_name,
        cost_coverage
    FROM results
    WHERE solver_env = '{{SolverEnv}}'
),
result_average as (
    select 
        hour,
        'average' as solver_name,
        avg(cost_coverage) as cost_coverage
    from production_coverage
    group by hour
)
SELECT *
FROM (
        SELECT *
        FROM production_coverage
        UNION
        SELECT *
        FROM result_average
    ) as _
WHERE hour > NOW() - INTERVAL '{{Interval Length}}' {{Units}}
ORDER BY cost_coverage DESC

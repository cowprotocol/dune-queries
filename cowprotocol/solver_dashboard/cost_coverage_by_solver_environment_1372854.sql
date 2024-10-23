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
    where block_time > NOW() - INTERVAL '2' MONTH
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
    where block_time > NOW() - INTERVAL '2' MONTH
),
results as (
  SELECT date(block_time) as day,
    solver_env,
    sum(fee) / sum(gas_cost) cost_coverage
  FROM (
      SELECT *
      FROM fee_and_cost_per_batch
      UNION
      SELECT *
      FROM failed_settlements
    ) as _
  GROUP by 
    date(block_time), solver_env
)
SELECT *
FROM results
WHERE solver_env in ('prod', 'barn')
ORDER BY cost_coverage DESC

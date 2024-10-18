WITH 
settlement_transactions AS (
    SELECT 
        date_trunc('{{Frequence}}', block_time) as time,
        "from" as solver,
        sum(CASE WHEN success THEN 1 ELSE 0 END) as successes,
        sum(CASE WHEN NOT success THEN 1 ELSE 0 END) as failures
    FROM ethereum.transactions
    WHERE block_time > NOW() - INTERVAL '{{LastNDays}}' day
    AND to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    AND position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
    group by "from", date_trunc('{{Frequence}}', block_time)
),
results as (
    SELECT 
        time,
        name as solver,
        environment,
        successes,
        failures,
        (case when successes = 0 then 0 else 1.0 * failures / (successes + failures) end) as failure_rate
    FROM settlement_transactions
        JOIN cow_protocol_ethereum.solvers 
            ON solver = address
)
SELECT * FROM results
WHERE environment in ('prod', 'barn')
order by time desc
-- Computes the weekly payment due for each connected builder
-- Parameters:
--  {{fee_computation_start}} - the start timestamp for the per block fee computation subquery
--  {{fee_computation_end}} - the end timestamp for the per block fee computation subquery
--  {{billing_date}} - the date at which the query is run

WITH block_range AS (
    SELECT
        MIN(number) AS start_block,
        MAX(number) + 1 AS end_block -- range is exclusive
    FROM ethereum.blocks
    WHERE
        time >= TIMESTAMP '{{billing_date}}' - INTERVAL '7' DAY
        AND time < TIMESTAMP '{{billing_date}}'
),

final_fee AS (
    -- TODO: remove between  2025/04/03 and 2025/04/08
    SELECT avg_block_fee_wei * 4 / 5 as avg_block_fee_wei
    FROM "query_4002039(start='{{fee_computation_start}}', end='{{fee_computation_end}}')"
),

-- selects the count of blocks each builder won in the billing period
builder_blocks AS (
    SELECT
        label,
        billing_address,
        COUNT(*) AS blocks_won
    FROM ethereum.raw_0004
    INNER JOIN query_4001804 AS info
        ON
            (CONTAINS(info.extra_data, FROM_HEX(block.extraData)) OR CONTAINS(info.builder_addresses, FROM_HEX(block.miner))) --noqa: RF01
            AND blockNumber >= start_block
            AND blockNumber < end_block
            AND blockNumber >= (SELECT start_block FROM block_range)
            AND blockNumber < (SELECT end_block FROM block_range)
    GROUP BY 1, 2
)

SELECT
    b.*,
    avg_block_fee_wei AS weekly_fee,
    blocks_won * avg_block_fee_wei AS amount_due_wei,
    blocks_won * avg_block_fee_wei / 1e18 AS amount_due_eth
FROM builder_blocks AS b
CROSS JOIN final_fee

-- Computes the monthly per block fee that MEVBlocker subscribed builders need to pay for every block they land.
-- Parameters:
--  {{start}} - the timestamp for which the analysis should start (inclusively)
--  {{end}} - the timestamp for which the analysis should end (exclusively)

WITH block_range AS (
    SELECT
        MIN(number) AS start_block,
        MAX(number) + 1 AS end_block -- range is exclusive
    FROM ethereum.blocks
    WHERE
        time >= TIMESTAMP '{{start}}'
        AND time < TIMESTAMP '{{end}}'
),

builders_extra_data AS (
    SELECT
        start_block,
        end_block,
        ed.extra_data_item
    FROM query_4001804
    CROSS JOIN UNNEST(extra_data) AS ed (extra_data_item)
),

builder_addresses AS (
    SELECT
        start_block,
        end_block,
        t.builder_address
    FROM query_4001804
    CROSS JOIN UNNEST(builder_addresses) AS t (builder_address)
),

-- select number of blocks from subscribed builders
blocks AS (
    SELECT COUNT(*) AS cnt_blocks
    FROM ethereum.raw_0004 AS b
    WHERE
        (
            FROM_HEX(b.block.extraData) IN (SELECT extra_data_item FROM builders_extra_data WHERE blockNumber >= start_block AND blockNumber < end_block) --noqa: RF01
            OR FROM_HEX(b.block.miner) IN (SELECT builder_address FROM builder_addresses WHERE blockNumber >= start_block AND blockNumber < end_block) --noqa: RF01
        )
        AND b.blockNumber >= (SELECT start_block FROM block_range)
        AND b.blockNumber < (SELECT end_block FROM block_range)
),

-- select total fee
fee AS (
    SELECT
        SUM(user_tip_wei) AS total_user_tip_wei,
        SUM(backrun_value_wei) AS total_backrun_value_wei,
        SUM(backrun_tip_wei) AS total_backrun_tip_wei,
        SUM(block_fee_wei) AS total_fee_wei
    FROM "query_3999838(start='{{start}}', end='{{end}}')"
)

-- compute average fee per block
SELECT
    f.*,
    b.cnt_blocks AS total_blocks,
    (f.total_fee_wei / b.cnt_blocks) AS avg_block_fee_wei,
    (f.total_fee_wei / b.cnt_blocks / 1e18) AS avg_block_fee
FROM fee AS f
CROSS JOIN blocks AS b

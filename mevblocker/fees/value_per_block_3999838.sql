-- Computes the value MEV Blocker transactions contribute for each block
-- Parameters:
--  {{start}} - the timestamp for which the analysis should start (inclusively)
--  {{end}} - the timestamp for which the analysis should end (exclusively)

WITH block_range AS (
    SELECT
        MIN(number) AS start_block,
        MAX(number) AS end_block
    FROM ethereum.blocks
    WHERE
        time >= TIMESTAMP '{{start}}'
        AND time < TIMESTAMP '{{end}}'
),

-- all mempool tx according to flashbots
mempool AS (
    SELECT DISTINCT hash
    FROM dune.flashbots.dataset_mempool_dumpster
    WHERE
        included_at_block_height >= (SELECT start_block FROM block_range)
        AND included_at_block_height < (SELECT end_block FROM block_range)

),

-- relevant mev blocker bundles during that timeframe
mev_blocker_filtered AS (
    SELECT *
    FROM mevblocker.raw_bundles
    WHERE
        blocknumber >= (SELECT start_block FROM block_range)
        AND blocknumber < (SELECT end_block FROM block_range)
),

-- all mev blocker tx that made it on chain
mev_blocker_tx AS (
    SELECT
        jt.row_number,
        et."from" AS tx_from,
        et.index,
        et.block_number,
        et.block_time,
        et.gas_used,
        et.gas_price,
        FROM_HEX(CAST(JSON_EXTRACT(mb.transactions, '$[0].hash') AS VARCHAR)) AS tx_1,
        FROM_HEX(CAST(JSON_EXTRACT(mb.transactions, '$[0].from') AS VARCHAR)) AS tx_from_1,
        FROM_HEX(jt.hash) AS hash
    FROM
        mev_blocker_filtered AS mb
    CROSS JOIN
        JSON_TABLE(
            mb.transactions,
            'lax $[*]' COLUMNS(
                row_number for ordinality,
                hash VARCHAR(255) PATH 'lax $.hash'
            )
        ) AS jt
    INNER JOIN ethereum.transactions AS et
        ON FROM_HEX(jt.hash) = et.hash
    WHERE
        block_number >= (SELECT start_block FROM block_range)
        AND block_number < (SELECT end_block FROM block_range)
),

-- find last tx in mev blocker bundle 
-- based on the assumption that only the last tx in a bundle can be a searcher transaction
last_tx_in_bundle AS (
    SELECT
        tx_1,
        MAX(row_number) AS last_tx_number,
        MIN(row_number) AS first_row
    FROM mev_blocker_tx
    GROUP BY 1
),

-- find which of the last tx in a bundle are not from the same address AS the original tx 
-- assume these are searcher tx
searcher_txs AS (
    SELECT
        m.tx_1,
        m.hash AS search_tx,
        m.index,
        m.block_number,
        m.block_time
    FROM mev_blocker_tx AS m
    INNER JOIN last_tx_in_bundle AS l
        ON
            m.tx_1 = l.tx_1
            AND last_tx_number = row_number
            AND first_row = 1 -- making sure the original transaction happened
    WHERE tx_from != tx_from_1
),

kickbacks AS (
    SELECT
        et.block_time,
        et.block_number,
        value AS backrun_value_wei,
        CAST(et.gas_used AS UINT256) * (et.gas_price - COALESCE(b.base_fee_per_gas, 0)) AS backrun_tip_wei
    FROM searcher_txs AS st
    INNER JOIN ethereum.transactions AS et
        ON
            st.block_number = et.block_number
            AND st.index + 1 = et.index
    INNER JOIN ethereum.blocks AS b
        ON
            st.block_number = number
            AND et."from" = b.miner
    WHERE
        et.block_number >= (SELECT start_block FROM block_range)
        AND et.block_number < (SELECT end_block FROM block_range)
),

-- all original (user) transactions, calculating the tip of these transactions
-- excluding transactions that were in the public mempool
user_tx AS (
    SELECT
        tx.block_time,
        tx.block_number,
        tx.hash,
        CAST(tx.gas_used AS UINT256) * (tx.gas_price - COALESCE(b.base_fee_per_gas, 0)) AS user_tip_wei
    FROM mev_blocker_tx AS tx
    LEFT JOIN ethereum.blocks AS b ON block_number = number
    WHERE
        tx.hash NOT IN (SELECT search_tx FROM searcher_txs)
        AND CAST(tx.hash AS VARCHAR) NOT IN (SELECT hash FROM mempool)
)

-- final calculation of the fee per block 
-- the calculation: 20% of (original_tx_tip + 1/9 of backrun value)
SELECT
    b.time AS block_time,
    b.number AS block_number,
    SUM(user_tip_wei) AS user_tip_wei,
    SUM(COALESCE(k.backrun_value_wei, 0)) AS backrun_value_wei,
    SUM(COALESCE(k.backrun_tip_wei, 0)) AS backrun_tip_wei,
    SUM(CAST(0.2 * (user_tip_wei + (COALESCE(k.backrun_tip_wei + k.backrun_value_wei, 0) / 9)) AS UINT256))
        AS block_fee_wei,
    ARRAY_AGG(ut.hash) AS txs
FROM ethereum.blocks AS b
INNER JOIN user_tx AS ut ON b.number = ut.block_number
LEFT JOIN kickbacks AS k ON b.number = k.block_number
GROUP BY 1, 2

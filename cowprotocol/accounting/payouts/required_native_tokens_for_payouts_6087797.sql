WITH protocol_fee_in_native_token AS (
    SELECT protocol_fee_in_native_token
    FROM "query_2601653(start_time='{{start_time}}', end_time='{{end_time}}', blockchain='{{blockchain}}')"
),

partner_fees AS (
    SELECT
        SUM(cow_dao_partner_fee_part) AS cow_dao_partner_fee_part,
        SUM(partner_fee_part) AS partner_fee_part
    FROM "query_3602560(start_time='{{start_time}}', end_time='{{end_time}}', blockchain='{{blockchain}}', result='per_recipient_partner_fees')"
),

conversion_prices AS (
    SELECT
        cow_price,
        native_token_price
    FROM dune.cowprotocol.result_accounting_cow_and_native_prices_per_chain
    WHERE
        blockchain = '{{blockchain}}'
        AND end_time > DATE_ADD('day', -1, CAST('{{end_time}}' AS timestamp))
        AND end_time < DATE_ADD('day', +1, CAST('{{end_time}}' AS timestamp))
),

auction_data AS (
    SELECT
        total_network_fee / POW(10,18) AS network_fee_eth,
        total_execution_cost / POW(10,18) AS execution_cost_eth,
        capped_payment / POW(10,18) AS primary_reward_eth
    FROM "query_5270914(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
),

aggregate AS (
    SELECT
        SUM(primary_reward_eth) AS primary_reward_eth,
        SUM(network_fee_eth) AS network_fee_eth,
        SUM(execution_cost_eth) AS execution_cost_eth
    FROM auction_data
),

extended_payout_data AS (
    SELECT
        primary_reward_eth,
        network_fee_eth,
        execution_cost_eth,
        network_fee_eth AS reimbursement_eth,
        (primary_reward_eth + network_fee_eth + execution_cost_eth) AS total_outgoing_eth,
        primary_reward_eth * (SELECT native_token_price / cow_price FROM conversion_prices) AS total_cow_reward
    FROM aggregate
),

buffer_params AS (
    SELECT
        SPLIT('{{blockchain}}', ',') AS blockchains,
        CAST('{{start_time}}' AS timestamp) AS start_time,
        CAST('{{end_time}}' AS timestamp) AS end_time
),

buffer_withdrawals AS (
    SELECT SUM(m.withdrawals_native * m.native_price_usd / m.eth_price_usd) AS withdrawals_eth
    FROM dune.cowprotocol.result_buffer_perf_reconciliation_weekly AS m -- not merging with historical data to save credits since we will only ever use this for current/previous week
    CROSS JOIN buffer_params AS bp
    WHERE IF(
        ARRAY_POSITION(bp.blockchains, 'All Supported Chains') > 0,
        true,
        ARRAY_POSITION(bp.blockchains, m.blockchain) > 0
    )
    AND m.week_start < bp.end_time
    AND DATE_ADD('day', 7, m.week_start) > bp.start_time
),

balance_payouts_safe AS (
    SELECT
        CASE '{{blockchain}}'
            WHEN 'arbitrum' THEN 0x66331f0b9cb30d38779c786bda5a3d57d12fba50
            WHEN 'polygon' THEN 0x66331f0b9cb30d38779c786bda5a3d57d12fba50
            ELSE 0xa03be496e67ec29bc62f01a428683d7f9c204930
        END AS holder
),

event_topics AS (
    SELECT
        0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef AS transfer_topic,
        0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c AS deposit_topic,
        0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65 AS withdrawal_topic,
        0x0000000000000000000000000000000000000000 AS zero_address
),

-- Shows the current balance, irrespective of the `end_time` parameter
-- This is a design choice as the query will be used to prepare for the payouts
-- We will want to know do we have enough balances to execute at the time of running the query
-- This way the balance keeps updating as funds are transferred to the safe
wrapped_native_logs AS (
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM ethereum.logs
    WHERE '{{blockchain}}' = 'ethereum' AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM gnosis.logs
    WHERE '{{blockchain}}' = 'gnosis' AND contract_address = 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM base.logs
    WHERE '{{blockchain}}' = 'base' AND contract_address = 0x4200000000000000000000000000000000000006
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM avalanche_c.logs
    WHERE '{{blockchain}}' = 'avalanche_c' AND contract_address = 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM bnb.logs
    WHERE '{{blockchain}}' = 'bnb' AND contract_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM linea.logs
    WHERE '{{blockchain}}' = 'linea' AND contract_address = 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM plasma.logs
    WHERE '{{blockchain}}' = 'plasma' AND contract_address = 0x6100e367285b01f48d07953803a2d8dca5d19873
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM ink.logs
    WHERE '{{blockchain}}' = 'ink' AND contract_address = 0x4200000000000000000000000000000000000006
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM arbitrum.logs
    WHERE '{{blockchain}}' = 'arbitrum' AND contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
    UNION ALL
    SELECT
        topic0,
        topic1,
        topic2,
        data
    FROM polygon.logs
    WHERE '{{blockchain}}' = 'polygon' AND contract_address = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
),

wrapped_native_events AS (
    SELECT
        l.topic0,
        ps.holder,
        SUBSTR(l.topic1, 13, 20) AS addr1, -- Transfer.from / Deposit.dst / Withdrawal.src
        SUBSTR(l.topic2, 13, 20) AS addr2, -- Transfer.to
        CAST(BYTEARRAY_TO_UINT256(l.data) AS double) AS wad
    FROM wrapped_native_logs AS l
    CROSS JOIN balance_payouts_safe AS ps
    CROSS JOIN event_topics AS t
    WHERE
        l.topic0 IN (t.transfer_topic, t.deposit_topic, t.withdrawal_topic)
        AND (SUBSTR(l.topic1, 13, 20) = ps.holder OR SUBSTR(l.topic2, 13, 20) = ps.holder)
),

wrapped_native_signed AS (
    SELECT
        CASE
            WHEN e.topic0 = t.transfer_topic AND e.addr2 = e.holder AND e.addr1 != t.zero_address THEN e.wad
            WHEN e.topic0 = t.transfer_topic AND e.addr1 = e.holder AND e.addr2 != t.zero_address THEN -e.wad
            WHEN e.topic0 = t.deposit_topic THEN e.wad
            WHEN e.topic0 = t.withdrawal_topic THEN -e.wad
            ELSE 0
        END AS signed_wad
    FROM wrapped_native_events AS e
    CROSS JOIN event_topics AS t
),

total_balance_native AS (
    SELECT COALESCE(SUM(signed_wad) / 1e18, 0) AS total_balance
    FROM wrapped_native_signed
)

SELECT
    p.protocol_fee_in_native_token AS protocol_fees,
    COALESCE(f.cow_dao_partner_fee_part, 0) + COALESCE(f.partner_fee_part, 0) AS partner_fees,
    e.native_token_transfer,
    COALESCE(b_w.withdrawals_eth, 0) AS withdrawals_eth,
    b.total_balance AS current_total_balance,
    COALESCE(p.protocol_fee_in_native_token, 0)
    + COALESCE(f.cow_dao_partner_fee_part, 0)
    + COALESCE(f.partner_fee_part, 0)
    + COALESCE(e.native_token_transfer, 0) AS total_outgoing_transfer
FROM protocol_fee_in_native_token AS p
CROSS JOIN partner_fees AS f
CROSS JOIN (
    SELECT
        CASE
            WHEN (network_fee_eth > 0 AND total_cow_reward < 0)
                THEN network_fee_eth + primary_reward_eth
            WHEN (network_fee_eth < 0 AND total_cow_reward > 0)
                THEN 0
            ELSE network_fee_eth
        END AS native_token_transfer
    FROM extended_payout_data
) AS e
CROSS JOIN buffer_withdrawals AS b_w
CROSS JOIN total_balance_native AS b;

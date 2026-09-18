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
)

SELECT
    p.protocol_fee_in_native_token AS protocol_fees,
    COALESCE(f.cow_dao_partner_fee_part, 0) + COALESCE(f.partner_fee_part, 0) AS partner_fees,
    e.native_token_transfer,
    COALESCE(p.protocol_fee_in_native_token, 0)
    + COALESCE(f.cow_dao_partner_fee_part, 0)
    + COALESCE(f.partner_fee_part, 0)
    + COALESCE(e.native_token_transfer, 0) AS total
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
) AS e;

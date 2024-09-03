WITH

bonding_pools (name, pool, initial_funder) AS (
    SELECT
        name,
        from_hex(pool),
        from_hex(funder)
    FROM (
        VALUES {{BondingPoolData}}
    ) AS _ (name, pool,funder)
),

first_event_after_timestamp AS (
    SELECT max(number)
    FROM
        ethereum.blocks
    WHERE
        time > cast('2024-08-20 00:00:00' AS timestamp) -- CIP-48 starts bonding pool timer at midnight UTC on 20/08/24
),

initial_vouches AS (
    SELECT
        evt_block_number,
        evt_index,
        solver,
        cowRewardTarget,
        bondingPool,
        sender,
        True AS active,
        rank() OVER (
            PARTITION BY
                solver,
                bondingPool,
                sender
            ORDER BY
                evt_block_number ASC,
                evt_index ASC
        ) AS rk
    FROM
        cow_protocol_ethereum.VouchRegister_evt_Vouch
    WHERE
        evt_block_number <= (
            SELECT *
            FROM
                first_event_after_timestamp
        )
        AND bondingPool IN (
            SELECT pool
            FROM
                bonding_pools
        )
        AND sender IN (
            SELECT initial_funder
            FROM
                bonding_pools
        )
),

joined_on_data AS (
    SELECT
        iv.solver,
        iv.cowRewardTarget AS reward_target,
        iv.bondingPool AS pool,
        iv.evt_block_number,
        iv.evt_index,
        iv.rk,
        True AS active
    FROM
        initial_vouches AS iv
    WHERE
        iv.rk = 1
),

latest_vouches AS (
    SELECT
        evt_block_number,
        evt_index,
        solver,
        cowRewardTarget,
        bondingPool,
        sender,
        rank() OVER (
            PARTITION BY
                solver,
                bondingPool,
                sender
            ORDER BY
                evt_block_number DESC,
                evt_index DESC
        ) AS rk,
        coalesce (event_type = 'Vouch', FALSE) AS active
    FROM
        (
            SELECT
                evt_block_number,
                evt_index,
                solver,
                cowRewardTarget,
                bondingPool,
                sender,
                'Vouch' AS event_type
            FROM
                cow_protocol_ethereum.VouchRegister_evt_Vouch
            WHERE
                evt_block_number <= (
                    SELECT *
                    FROM
                        first_event_after_timestamp
                )
                AND bondingPool IN (
                    SELECT pool
                    FROM
                        bonding_pools
                )
                AND sender IN (
                    SELECT initial_funder
                    FROM
                        bonding_pools
                )
            UNION DISTINCT
            SELECT
                evt_block_number,
                evt_index,
                solver,
                Null AS cowRewardTarget, -- Invalidation does not have a reward target
                bondingPool,
                sender,
                'InvalidateVouch' AS event_type
            FROM
                cow_protocol_ethereum.VouchRegister_evt_InvalidateVouch
            WHERE
                evt_block_number <= (
                    SELECT *
                    FROM
                        first_event_after_timestamp
                )
                AND bondingPool IN (
                    SELECT pool
                    FROM
                        bonding_pools
                )
                AND sender IN (
                    SELECT initial_funder
                    FROM
                        bonding_pools
                )
        ) AS unioned_events
),

valid_vouches AS (
    SELECT
        lv.solver,
        lv.cowRewardTarget AS reward_target,
        lv.bondingPool AS pool
    FROM
        latest_vouches AS lv
    WHERE
        lv.rk = 1
        AND lv.active = True
),

joined_on AS (
    SELECT
        jd.solver,
        jd.reward_target,
        jd.pool,
        bp.name AS pool_name,
        b.time AS joined_on
    FROM
        joined_on_data AS jd
    INNER JOIN ethereum.blocks AS b ON jd.evt_block_number = b.number
    INNER JOIN bonding_pools AS bp ON jd.pool = bp.pool
),

named_results AS (
    SELECT
        jd.solver,
        jd.pool_name,
        jd.pool,
        jd.joined_on,
        concat(environment, '-', s.name) AS solver_name,
        date_diff('day', date(jd.joined_on), date(now())) AS days_in_pool
    FROM
        joined_on AS jd
    INNER JOIN cow_protocol_ethereum.solvers AS s ON jd.solver = s.address
    INNER JOIN valid_vouches
        AS vv ON jd.solver = vv.solver
    AND jd.pool = vv.pool
),

ranked_named_results AS (
    SELECT
        nr.solver,
        nr.solver_name,
        nr.pool_name,
        nr.pool,
        nr.joined_on,
        nr.days_in_pool,
        row_number() OVER (
            PARTITION BY
                nr.solver_name
            ORDER BY
                nr.joined_on DESC
        ) AS rn,
        count(*) OVER (
            PARTITION BY
                nr.solver_name
        ) AS solver_name_count
    FROM
        named_results AS nr
),

filtered_named_results AS (
    SELECT
        rnr.solver,
        rnr.solver_name,
        rnr.pool,
        rnr.joined_on,
        rnr.days_in_pool,
        CASE
            WHEN rnr.solver_name_count > 1 THEN 'Colocation'
            ELSE rnr.pool_name
        END AS pool_name,
        CASE
            WHEN rnr.solver_name_count > 1 THEN date_add('month', 3, rnr.joined_on) -- Add 3 month grace period for colocated solvers
            ELSE greatest(
                date_add('month', 6, rnr.joined_on), -- Add 6 month grace period to joined_on for non colocated solvers
                TIMESTAMP '2024-08-20 00:00:00' -- Introduction of CIP-48
            )
        END AS expires
    FROM
        ranked_named_results AS rnr
    WHERE
        rnr.rn = 1
)

SELECT
    fnr.solver,
    fnr.solver_name,
    fnr.pool_name,
    fnr.pool,
    fnr.joined_on,
    fnr.days_in_pool,
    CASE
        WHEN fnr.pool_name = 'Gnosis' THEN TIMESTAMP '2028-10-08 00:00:00'
        ELSE fnr.expires
    END AS expires,
    coalesce(
        now() > fnr.expires
        AND fnr.pool_name != 'Gnosis', FALSE) AS service_fee
FROM
    filtered_named_results AS fnr;

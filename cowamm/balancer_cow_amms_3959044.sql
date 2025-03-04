-- This is part of a base query for monitoring Balancer CoW AMMs
-- It indexes all Balancer CoW AMMs on ethereum and gnosis and arbitrum and base
--
-- the final table has columns
-- - created_at: the creation timestamp
-- - blockchain: 'ethereum' or 'gnosis' or 'arbitrum' or 'base'
-- - address: address of Balancer CoW AMM
-- - token_1_address: address of token with smaller address
-- - token_2_address: address of token with larger address

with cowamm_creations_ethereum as (
    select varbinary_substring(topic1, 1 + 12, 20) as address
    from ethereum.logs
    where (
        contract_address in (0x23fcC2166F991B8946D195de53745E1b804C91B7, 0xf76c421bAb7df8548604E60deCCcE50477C10462) -- this needs to be changed when a new factory contract is deployed
        and topic0 = 0x0d03834d0d86c7f57e877af40e26f176dc31bd637535d4ba153d1ac9de88a7ea
    )
    and block_time >= cast('2024-07-29 00:00:00' as timestamp)
),

cowamm_weights_ethereum as (
    select
        logs.contract_address,
        varbinary_substring(data, 81, 20) as token,
        varbinary_to_uint256(varbinary_substring(data, 133, 32)) as weight
    from cowamm_creations_ethereum
    inner join ethereum.logs
        on logs.contract_address = address
    where
        topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
),

-- get tokens added via bind (0xe4e1e538) on the CoW AMM address
cowamms_ethereum_tokens as (
    select
        'ethereum' as blockchain,
        contract_address as address,
        min(block_time) as created_at,
        min(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_1_address,
        max(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_2_address
    from
        ethereum.logs
    where
        contract_address in (select address from cowamm_creations_ethereum)
        and topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
        and block_time >= cast('2024-07-29 00:00:00' as timestamp)
    group by 1, 2
),

cowamms_ethereum as (
    select
        blockchain,
        address,
        created_at,
        token_1_address,
        token_2_address,
        100 * w1.weight / (w1.weight + w2.weight) as token_1_weight,
        100 * w2.weight / (w1.weight + w2.weight) as token_2_weight
    from cowamms_ethereum_tokens
    inner join cowamm_weights_ethereum as w1
        on
            w1.contract_address = address
            and w1.token = token_1_address
    inner join cowamm_weights_ethereum as w2
        on
            w2.contract_address = address
            and w2.token = token_1_address
),


-- on gnosis
cowamm_creations_gnosis as (
    select varbinary_substring(topic1, 1 + 12, 20) as address
    from gnosis.logs
    where (
        contract_address in (0x23fcC2166F991B8946D195de53745E1b804C91B7, 0x703Bd8115E6F21a37BB5Df97f78614ca72Ad7624) -- this needs to be changed when a new factory contract is deployed
        and topic0 = 0x0d03834d0d86c7f57e877af40e26f176dc31bd637535d4ba153d1ac9de88a7ea
    )
    and block_time >= cast('2024-07-29 00:00:00' as timestamp)
),

cowamm_weights_gnosis as (
    select
        logs.contract_address,
        varbinary_substring(data, 81, 20) as token,
        varbinary_to_uint256(varbinary_substring(data, 133, 32)) as weight
    from cowamm_creations_gnosis
    inner join gnosis.logs
        on logs.contract_address = address
    where
        topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
),


cowamms_gnosis_tokens as (
    select
        'gnosis' as blockchain,
        contract_address as address,
        min(block_time) as created_at,
        min(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_1_address,
        max(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_2_address
    from
        gnosis.logs
    where
        contract_address in (select address from cowamm_creations_gnosis)
        and topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
        and block_time >= cast('2024-07-29 00:00:00' as timestamp)
    group by 1, 2
),

cowamms_gnosis as (
    select
        blockchain,
        address,
        created_at,
        token_1_address,
        token_2_address,
        100 * w1.weight / (w1.weight + w2.weight) as token_1_weight,
        100 * w2.weight / (w1.weight + w2.weight) as token_2_weight
    from cowamms_gnosis_tokens
    inner join cowamm_weights_gnosis as w1
        on
            w1.contract_address = address
            and w1.token = token_1_address
    inner join cowamm_weights_gnosis as w2
        on
            w2.contract_address = address
            and w2.token = token_1_address
),

-- on arbitrum
cowamm_creations_arbitrum as (
    select varbinary_substring(topic1, 1 + 12, 20) as address
    from arbitrum.logs
    where (
        contract_address in (0xe0e2ba143ee5268da87d529949a2521115987302)
        and topic0 = 0x0d03834d0d86c7f57e877af40e26f176dc31bd637535d4ba153d1ac9de88a7ea
    )
    and block_time >= cast('2024-09-01 00:00:00' as timestamp)
),

cowamm_weights_arbitrum as (
    select
        logs.contract_address,
        varbinary_substring(data, 81, 20) as token,
        varbinary_to_uint256(varbinary_substring(data, 133, 32)) as weight
    from cowamm_creations_arbitrum
    inner join arbitrum.logs
        on logs.contract_address = address
    where
        topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
),

cowamms_arbitrum_tokens as (
    select
        'arbitrum' as blockchain,
        contract_address as address,
        min(block_time) as created_at,
        min(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_1_address,
        max(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_2_address
    from
        arbitrum.logs
    where
        contract_address in (select address from cowamm_creations_arbitrum)
        and topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
        and block_time >= cast('2024-09-01 00:00:00' as timestamp)
    group by 1, 2
),

cowamms_arbitrum as (
    select
        blockchain,
        address,
        created_at,
        token_1_address,
        token_2_address,
        100 * w1.weight / (w1.weight + w2.weight) as token_1_weight,
        100 * w2.weight / (w1.weight + w2.weight) as token_2_weight
    from cowamms_arbitrum_tokens
    inner join cowamm_weights_arbitrum as w1
        on
            w1.contract_address = address
            and w1.token = token_1_address
    inner join cowamm_weights_arbitrum as w2
        on
            w2.contract_address = address
            and w2.token = token_1_address
),


-- on base
cowamm_creations_base as (
    select varbinary_substring(topic1, 1 + 12, 20) as address
    from base.logs
    where (
        contract_address in (0x03362f847B4fAbC12e1Ce98b6b59F94401E4588e)
        and topic0 = 0x0d03834d0d86c7f57e877af40e26f176dc31bd637535d4ba153d1ac9de88a7ea
    )
    and block_time >= cast('2024-12-01 00:00:00' as timestamp)
),

cowamm_weights_base as (
    select
        logs.contract_address,
        varbinary_substring(data, 81, 20) as token,
        varbinary_to_uint256(varbinary_substring(data, 133, 32)) as weight
    from cowamm_creations_base
    inner join base.logs
        on logs.contract_address = address
    where
        topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
),

cowamms_base_tokens as (
    select
        'base' as blockchain,
        contract_address as address,
        min(block_time) as created_at,
        min(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_1_address,
        max(varbinary_substring(data, 5 + 2 * 32 + 12, 20)) as token_2_address
    from
        base.logs
    where
        contract_address in (select address from cowamm_creations_base)
        and topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
        and block_time >= cast('2024-12-01 00:00:00' as timestamp)
    group by 1, 2
),

cowamms_base as (
    select
        blockchain,
        address,
        created_at,
        token_1_address,
        token_2_address,
        100 * w1.weight / (w1.weight + w2.weight) as token_1_weight,
        100 * w2.weight / (w1.weight + w2.weight) as token_2_weight
    from cowamms_base_tokens
    inner join cowamm_weights_base as w1
        on
            w1.contract_address = address
            and w1.token = token_1_address
    inner join cowamm_weights_base as w2
        on
            w2.contract_address = address
            and w2.token = token_1_address
),


-- combine data for different chains
cowamms as (
    select * from cowamms_ethereum
    union all
    select * from cowamms_gnosis
    union all
    select * from cowamms_arbitrum
    union all
    select * from cowamms_base
)

select * from cowamms

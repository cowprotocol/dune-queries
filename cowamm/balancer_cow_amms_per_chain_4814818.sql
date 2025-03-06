-- This query gets all the cowamms created after the Balancer AMM launch on July 29, 2024 for a specific chain
--Parameters
--  {{blockchain}}: The blockchain to query
-- Hard coded start for the events scan to '2024-07-29', month of the Balancer AMMs launch

with cowamm_creations as (
    select varbinary_substring(topic1, 1 + 12, 20) as address
    from {{blockchain}}.logs
    where (
        contract_address in ( --the factory contracts
            0x23fcC2166F991B8946D195de53745E1b804C91B7, --ethereum
            0xf76c421bAb7df8548604E60deCCcE50477C10462, --ethereum
            0x23fcC2166F991B8946D195de53745E1b804C91B7, --gnosis
            0x703Bd8115E6F21a37BB5Df97f78614ca72Ad7624, --gnosis
            0xe0e2ba143ee5268da87d529949a2521115987302, --arbitrum
            0x03362f847B4fAbC12e1Ce98b6b59F94401E4588e --base
        )
        and topic0 = 0x0d03834d0d86c7f57e877af40e26f176dc31bd637535d4ba153d1ac9de88a7ea
    )
    and block_time >= cast('2024-07-29 00:00:00' as timestamp)
),


-- Bind events for tokens
cowamm_binds as (
    select
        logs.contract_address,
        block_time,
        varbinary_substring(data, 81, 20) as token,
        varbinary_to_uint256(varbinary_substring(data, 133, 32)) as weight
    from cowamm_creations
    inner join {{blockchain}}.logs
        on logs.contract_address = address
    where
        topic0 = 0xe4e1e53800000000000000000000000000000000000000000000000000000000
        and block_time >= cast('2024-07-29 00:00:00' as timestamp)
),

-- get the token pair for the cow amms
cowamms_tokens as (
    select
        '{{blockchain}}' as blockchain,
        contract_address as address,
        min(block_time) as created_at,
        min(token) as token_0_address,
        max(token) as token_1_address
    from
        cowamm_binds
    group by 1, 2
),

cowamms as (
    select
        blockchain,
        address,
        created_at,
        token_0_address,
        token_1_address,
        100 * b0.weight / (b0.weight + b1.weight) as token_0_weight,
        100 * b1.weight / (b0.weight + b1.weight) as token_1_weight
    from cowamms_tokens
    inner join cowamm_binds as b0
        on
            b0.contract_address = address
            and b0.token = token_0_address
    inner join cowamm_binds as b1
        on
            b1.contract_address = address
            and b1.token = token_1_address
)

select * from cowamms

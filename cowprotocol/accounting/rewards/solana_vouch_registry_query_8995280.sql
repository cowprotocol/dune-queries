-- Query that fetches the list of solver addresses that are active
-- and are properly vouched for by a full bonding pool on Solana
-- Parameters:
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)

with last_block_before_timestamp as (
    select end_block from "query_3333356(blockchain='ethereum',start_time='2018-01-01 00:00:00',end_time='{{end_time}}')"
),

full_bonding_pools as (
    select
        pool_address,
        pool_name,
        creator
    from query_4056263
),

vouches as (
    select
        v.evt_block_number,
        v.evt_index,
        to_base58(v.solver) as solver,
        v.cowrewardtarget as cow_reward_target,
        to_base58(v.solanarewardtarget) as solana_reward_target,
        fp.pool_address,
        fp.pool_name,
        fp.creator,
        True as active
    from cow_protocol_ethereum.solanavouchregister_evt_vouch as v
    inner join full_bonding_pools as fp
        on v.bondingpool = fp.pool_address and v.sender = fp.creator
    where evt_block_number <= (select * from last_block_before_timestamp)
),

invalidations as (
    select
        v.evt_block_number,
        v.evt_index,
        to_base58(v.solver) as solver,
        Null as cow_reward_target,  -- This is just to align with vouches to take a union
        Null as solana_reward_target,
        fp.pool_address,
        fp.pool_name,
        fp.creator,
        False as active
    from cow_protocol_ethereum.solanavouchregister_evt_invalidatevouch as v
    inner join full_bonding_pools as fp
        on v.bondingpool = fp.pool_address and v.sender = fp.creator
    where evt_block_number <= (select * from last_block_before_timestamp)
),

-- Intermediate helper table
vouches_and_invalidations as (
    select * from vouches
    union distinct
    select * from invalidations
),

-- At this point we have excluded all arbitrary vouches (i.e., those not from initial funders of recognized pools)
-- The next query ranks (solver, pool_address, creator) by most recent (vouch or invalidation)
-- and yields as rank 1, the current "active" status of the triplet.
ranked_vouches as (
    select
        *,
        rank() over (
            partition by solver, pool_address, creator
            order by evt_block_number desc, evt_index desc
        ) as rk
    from vouches_and_invalidations
),

-- This will contain all latest active vouches,
-- but could still contain solvers with multiplicity > 1 for different pools.
-- Rank here again by solver, and time, in decreasing order.
current_active_vouches as (
    select
        *,
        rank() over (
            partition by solver
            order by evt_block_number desc, evt_index desc
        ) as time_rank
    from ranked_vouches
    where
        rk = 1
        and active = True
),

-- To filter for the case of "same solver, different pool",
-- rank the current_active vouches and choose the latest
valid_vouches as (
    select
        solver,
        cow_reward_target,
        solana_reward_target,
        pool_address,
        pool_name
    from current_active_vouches
    where time_rank = 1
)

select * from valid_vouches


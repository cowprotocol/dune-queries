-- Query that fetches the list of solver addresses that are active
-- and are properly vouched for by a full bonding pool
-- Parameters:
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
with
last_block_before_timestamp as (
    select end_block from "query_3333356(start_time='2018-01-01 00:00',end_time='{{end_time}}')"
),

-- Query Logic Begins here!
vouches as (
    select
        evt_block_number,
        evt_index,
        solver,
        cowRewardTarget as reward_target,
        pool,
        sender,
        True as active
    from cow_protocol_ethereum.VouchRegister_evt_Vouch
    inner join query_4056263
        on
            pool = bondingPool
            and sender = initial_funder
    where evt_block_number <= (select * from last_block_before_timestamp)
),

invalidations as (
    select
        evt_block_number,
        evt_index,
        solver,
        Null as reward_target,  -- This is just ot align with vouches to take a union
        pool,
        sender,
        False as active
    from cow_protocol_ethereum.VouchRegister_evt_InvalidateVouch
    inner join query_4056263
        on
            pool = bondingPool
            and sender = initial_funder
    where evt_block_number <= (select * from last_block_before_timestamp)
),

-- At this point we have excluded all arbitrary vouches (i.e. those not from initial funders of recognized pools)
-- This ranks (solver, pool, sender) by most recent (vouch or invalidation)
-- and yields as rank 1, the current "active" status of the triplet.
vouches_and_invalidations as (
    select * from vouches
    union distinct
    select * from invalidations
),

ranked_vouches as (
    select
        *,
        rank() over (
            partition by solver, pool, sender
            order by evt_block_number desc, evt_index desc
        ) as rk
    from vouches_and_invalidations
),

-- This will contain all latest active vouches,
-- but could still contain solvers with multiplicity > 1 for different pools.
-- Rank here again by solver, and time.
current_active_vouches as (
    select
        *,
        rank() over (
            partition by solver
            order by evt_block_number, evt_index
        ) as time_rank
    from ranked_vouches
    where
        rk = 1
        and active = True
),

-- To filter for the case of "same solver, different pool",
-- rank the current_active vouches and choose the earliest
valid_vouches as (
    select
        solver,
        reward_target,
        pool
    from current_active_vouches
    where time_rank = 1
),

named_results as (
    select
        solver,
        reward_target,
        vv.pool as bonding_pool,
        bp.pool_name,
        concat(environment, '-', s.name) as solver_name
    from valid_vouches as vv
    inner join cow_protocol_ethereum.solvers as s
        on address = solver
    inner join query_4056263 as bp
        on vv.pool = bp.pool
)

select * from {{vouch_cte_name}}

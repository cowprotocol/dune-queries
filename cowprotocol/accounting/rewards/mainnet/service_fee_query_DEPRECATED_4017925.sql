------------ DEPRECATED!
------------
------------
------------
with

-- Add colocated solvers here
colocated_solvers as (
    select
        'prod-Barter' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-08-21 07:15:00' as joined_on,
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool,
        from_hex('0xC7899Ff6A3aC2FF59261bD960A8C880DF06E1041') as solver
    union all
    select
        'barn-Barter' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-08-21 07:15:00' as joined_on,
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool,
        from_hex('0xC7899Ff6A3aC2FF59261bD960A8C880DF06E1041') as solver
    union all
    select
        'prod-Copium_Capital' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-07-25 07:42:00' as joined_on,
        from_hex('0xc5Dc06423f2dB1B11611509A5814dD1b242268dd') as pool,
        from_hex('0x008300082C3000009e63680088f8c7f4D3ff2E87') as solver
    union all
    select
        'prod-Rizzolver' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-10 02:03:00' as joined_on,
        from_hex('0x0Deb0Ae9c4399C51289adB1f3ED83557A56dF657') as pool,
        from_hex('0x9DFc9Bb0FfF2dc96728D2bb94eaCee6ba3592351') as solver
    union all
    select
        'barn-Rizzolver' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-10 02:03:00' as joined_on,
        from_hex('0x0deb0ae9c4399c51289adb1f3ed83557a56df657') as pool,
        from_hex('0x26B5e3bF135D3Dd05A220508dD61f25BF1A47cBD') as solver
    union all
    select
        'prod-Portus' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-21 03:33:00' as joined_on,
        from_hex('0x3075F6aab29D92F8F062A83A0318c52c16E69a60') as pool,
        from_hex('0x6bf97aFe2D2C790999cDEd2a8523009eB8a0823f') as solver
    union all
    select
        'barn-Portus' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-21 03:33:00' as joined_on,
        from_hex('0x3075F6aab29D92F8F062A83A0318c52c16E69a60') as pool,
        from_hex('0x5131590ca2E9D3edC182581352b289dcaE83430c') as solver
    union all
    select
        'prod-Fractal' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-29 11:57:00' as joined_on,
        from_hex('0xDdb0a7BeBF71Fb5d3D7FB9B9B0804beDdf9C1C88') as pool,
        from_hex('0x95480d3f27658e73b2785d30beb0c847d78294c7') as solver
    union all
    select
        'barn-Fractal' as solver_name,
        'Reduced-Bonding' as pool_name,
        timestamp '2024-10-29 11:57:00' as joined_on,
        from_hex('0xDdb0a7BeBF71Fb5d3D7FB9B9B0804beDdf9C1C88') as pool,
        from_hex('0x2a2883ade8ce179265f12fc7b48a4b50b092f1fd') as solver
),

bonding_pools as (
    select
        pool_address as pool,
        pool_name,
        initial_funder
    from "query_4056263"
),

first_event_after_timestamp as (
    select max(number)
    from
        ethereum.blocks
    where
        time > cast('2024-08-20 00:00:00' as timestamp) -- CIP-48 starts bonding pool timer at midnight UTC on 20/08/24
),

initial_vouches as (
    select
        evt_block_number,
        evt_index,
        solver,
        cowrewardtarget,
        bondingpool,
        sender,
        true as active,
        rank() over (
            partition by
                solver,
                bondingpool,
                sender
            order by
                evt_block_number asc,
                evt_index asc
        ) as rk
    from
        cow_protocol_ethereum.VouchRegister_evt_Vouch
    where
        evt_block_number <= (
            select *
            from
                first_event_after_timestamp
        )
        and bondingpool in (
            select pool
            from
                bonding_pools
        )
        and sender in (
            select initial_funder
            from
                bonding_pools
        )
),

joined_on_data as (
    select
        iv.solver,
        iv.cowrewardtarget as reward_target,
        iv.bondingpool as pool,
        iv.evt_block_number,
        iv.evt_index,
        iv.rk,
        true as active
    from
        initial_vouches as iv
    where
        iv.rk = 1
),

joined_on_with_colocated as (
    select
        solver,
        reward_target,
        pool,
        evt_block_number,
        evt_index,
        rk,
        active
    from joined_on_data

    union all

    -- Add hardcoded colocated solvers
    select
        c.solver,
        null as reward_target,
        c.pool,
        null as evt_block_number,
        null as evt_index,
        1 as rk,
        true as active
    from colocated_solvers as c
),

latest_vouches as (
    select
        evt_block_number,
        evt_index,
        solver,
        cowrewardtarget,
        bondingpool,
        sender,
        rank() over (
            partition by
                solver,
                bondingpool,
                sender
            order by
                evt_block_number desc,
                evt_index desc
        ) as rk,
        coalesce(event_type = 'Vouch', false) as active
    from
        (
            select
                evt_block_number,
                evt_index,
                solver,
                cowrewardtarget,
                bondingpool,
                sender,
                'Vouch' as event_type
            from
                cow_protocol_ethereum.VouchRegister_evt_Vouch
            where
                evt_block_number <= (
                    select *
                    from
                        first_event_after_timestamp
                )
                and bondingpool in (
                    select pool
                    from
                        bonding_pools
                )
                and sender in (
                    select initial_funder
                    from
                        bonding_pools
                )
            union distinct
            select
                evt_block_number,
                evt_index,
                solver,
                null as cowrewardtarget, -- Invalidation does not have a reward target
                bondingpool,
                sender,
                'InvalidateVouch' as event_type
            from
                cow_protocol_ethereum.VouchRegister_evt_InvalidateVouch
            where
                evt_block_number <= (
                    select *
                    from
                        first_event_after_timestamp
                )
                and bondingpool in (
                    select pool
                    from
                        bonding_pools
                )
                and sender in (
                    select initial_funder
                    from
                        bonding_pools
                )
        ) as unioned_events
),

valid_vouches as (
    select
        lv.solver,
        lv.cowrewardtarget as reward_target,
        lv.bondingpool as pool
    from
        latest_vouches as lv
    where
        lv.rk = 1
        and lv.active = true
),

joined_on as (
    select
        jd.solver,
        jd.reward_target,
        jd.pool,
        bp.pool_name,
        b.time as joined_on
    from
        joined_on_with_colocated as jd
    inner join ethereum.blocks as b on jd.evt_block_number = b.number
    inner join bonding_pools as bp on jd.pool = bp.pool
),

named_results as (
    select
        jd.solver,
        jd.pool_name,
        jd.pool,
        jd.joined_on,
        concat(environment, '-', s.name) as solver_name,
        date_diff('day', date(jd.joined_on), date(now())) as days_in_pool
    from
        joined_on as jd
    inner join cow_protocol_ethereum.solvers as s on jd.solver = s.address
    inner join valid_vouches as vv
        on
            jd.solver = vv.solver
            and jd.pool = vv.pool

    union all

    select
        c.solver,
        c.pool_name,
        c.pool,
        c.joined_on,
        c.solver_name,
        date_diff('day', date(c.joined_on), date(now())) as days_in_pool
    from colocated_solvers as c
),

ranked_named_results as (
    select
        nr.solver,
        nr.solver_name,
        nr.pool_name,
        nr.pool,
        nr.joined_on,
        nr.days_in_pool,
        row_number() over (
            partition by
                nr.solver_name
            order by
                nr.joined_on desc
        ) as rn,
        count(*) over (
            partition by
                nr.solver_name
        ) as solver_name_count
    from
        named_results as nr
),

filtered_named_results as (
    select
        rnr.solver,
        rnr.solver_name,
        rnr.pool,
        rnr.joined_on,
        rnr.days_in_pool,
        rnr.pool_name,
        case
            when rnr.pool_name = 'Reduced-Bonding' then date_add('month', 3, rnr.joined_on) -- Add 3 month grace period for colocated solvers
            else greatest(
                date_add('month', 6, rnr.joined_on), -- Add 6 month grace period to joined_on for non colocated solvers
                timestamp '2024-08-20 00:00:00' -- Introduction of CIP-48
            )
        end as expires
    from
        ranked_named_results as rnr
    where
        rnr.rn = 1
)

select
    fnr.solver,
    fnr.solver_name,
    fnr.pool_name,
    fnr.pool,
    fnr.joined_on,
    fnr.days_in_pool,
    case
        when fnr.pool_name = 'Gnosis' then timestamp '2028-10-08 00:00:00'
        else fnr.expires
    end as expires,
    coalesce(
        now() > fnr.expires
        and fnr.pool_name != 'Gnosis', false
    ) as service_fee
from
    filtered_named_results as fnr;

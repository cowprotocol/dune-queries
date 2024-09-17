-- This query computes which solvers need to pay service fee
-- by breaking down the calculation in small (and somewhat tedious) steps
-- We stress the query is not efficient (and recomputes same things along the way)
-- but we believe it is at least straightforward to check its correctness, and given
-- that it's fast anyways, we decided to go with this.

-- Parameters:
-- {{time}}: the date that we want to evaluate whether the service fee needs to be applied (needs to be start of accounting period)

with
-- we first compute all solvers of the CoW DAO bonding pool (this includes the ones that have their own reduced subpool)
-- and only look at their names. Note that this is necessary to identify when a solver first joined the pool, as many solvers
-- have changes accounts multiple times 
active_cow_dao_solver_names as (
    select distinct substring(solver_name, 6, 100) as solver_name
    -- here we remove the "prod-" and "barn-" prefix so as to only work with the actual solver name,
    from
        "query_1541516(vouch_cte_name='named_results',end_time='{{time}}')"
    where
        pool_address = 0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6 -- CoW DAO bonding pool address
),

-- we now take one step back and look at all solver accounts (not only the active ones) that have joined the CoW DAO bonding pool at some point,
-- and we recover the date of them joining
all_cow_dao_solvers as (
    select
        s.name as solver_name,
        v.solver,
        v.evt_block_time,
        v.evt_block_number
    from
        cow_protocol_ethereum.VouchRegister_evt_Vouch as v
    inner join cow_protocol_ethereum.solvers as s on v.solver = s.address
    where
        v.bondingPool = 0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6
),

-- we now join the previous two tables so that we can associate the name of an active solver under the CoW DAO bonding pool
-- with its "real" join time, i.e., the first time that it joined the pool (using potentially an account that changed on the way)
join_date_per_cow_dao_solver as (
    select
        solver_name,
        min(evt_block_time) as join_time
    from
        all_cow_dao_solvers
    group by
        solver_name
),

-- we now create an intermediate table to put back the "prod/barn" prefix to the solver names
-- as this will make it easier to join with the rest of the tables that we want to do join
active_cow_dao_solvers_join_date as (
    select
        concat('prod-', jdpcds.solver_name) as solver_name,
        jdpcds.join_time
    from
        join_date_per_cow_dao_solver as jdpcds
    inner join active_cow_dao_solver_names as acdsn on jdpcds.solver_name = acdsn.solver_name
    union distinct
    select
        concat('barn-', jdpcds.solver_name) as solver_name,
        jdpcds.join_time
    from
        join_date_per_cow_dao_solver as jdpcds
    inner join active_cow_dao_solver_names as acdsn on jdpcds.solver_name = acdsn.solver_name
),

-- this is now the first calculation of whether a solver in the CoW DAO bonding pool needs to pay a service fee.
-- note that we assume here no solver has its own subpool, and we will correct that later on.
active_cow_dao_solvers_service_fee as (
    select
        solver_name,
        case
            when join_time > date_add('month', -6, cast('{{time}}' as timestamp)) then false
            else true
        end as pay_service_fee
    from
        active_cow_dao_solvers_join_date
),

-- we now create a table of all active solvers, not just the ones in the CoW DAO bonding pool,
-- where we still pretend there are no reduced bonding pools
all_active_solvers_service_fee_prelim as (
    select
        v.solver,
        v.reward_target,
        v.pool_address,
        v.pool_name,
        v.solver_name,
        coalesce(acdssf.pay_service_fee, false) as pay_service_fee
    from
        "query_1541516(vouch_cte_name='named_results',end_time='{{time}}')" as v
    left outer join active_cow_dao_solvers_service_fee as acdssf on v.solver_name = acdssf.solver_name
),

-- we now create another table that only looks at the reduced subpools and computes whether the solvers
-- with subpools need to pay a service fee or not
reduced_bonding_pool_solvers_service_fee as (
    select
        pool_name,
        solver_address,
        case
            when creation_date > date_add('month', -3, cast('{{time}}' as timestamp)) then false
            else true
        end as pay_service_fee
    from query_4065709
),

-- finally, we join the prelim table with the above table and we define that a solver with a subpool needs to pay
-- a servive fee only if it is at least 6 months in the CoW DAO bonding pool AND >= 3 months have passed since the creation
-- of the subpool
all_active_solvers_service_fee_final as (
    select
        p.solver,
        p.reward_target,
        p.pool_address,
        p.solver_name,
        coalesce(rbp.pool_name, p.pool_name) as pool_name,
        coalesce(rbp.pay_service_fee and p.pay_service_fee, p.pay_service_fee) as pay_service_fee
    from all_active_solvers_service_fee_prelim as p left outer join reduced_bonding_pool_solvers_service_fee as rbp on p.solver = rbp.solver_address
)

select * from all_active_solvers_service_fee_final

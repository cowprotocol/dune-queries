-- This query computes which solvers need to pay service fee
-- by breaking down the calculation in small (and somewhat tedious) steps
-- We stress the query is not efficient (and recomputes same things along the way)
-- but we believe it is at least straightforward to check its correctness, and given
-- that it's fast anyways, we decided to go with this.

-- Parameters:
-- {{time}}: the end date of an accounting week

with
-- we first compute all active solvers of the CoW DAO bonding pool (this includes the ones that have their own reduced subpool)
active_cow_dao_solver_names as (
    select --noqa: ST06
        case
            when solver_name = 'new-Uncatalogued' then cast(solver as varchar)
            else substring(solver_name, 6, 100)
        end as solver_name,
        case
            when solver_name = 'new-Uncatalogued' then 'new'
            else substring(solver_name, 1, 4)
        end as environment,
        solver as solver_address,
        pool_name
    from
        "query_1541516(vouch_cte_name='named_results',end_time='{{time}}')"
    where
        pool_address = 0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6 -- CoW DAO bonding pool address
),

-- we now take one step back and look at all solver accounts (not only the active ones) that have joined the CoW DAO bonding pool at some point,
-- and we recover the date of them joining
all_cow_dao_solvers as (
    select --noqa: ST06
        case
            when s.environment = 'new' then cast(v.solver as varchar)
            else s.name
        end as solver_name,
        s.environment,
        v.evt_block_time
    from
        cow_protocol_ethereum.VouchRegister_evt_Vouch as v
    inner join cow_protocol_ethereum.solvers as s on v.solver = s.address
    where
        v.bondingPool = 0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6
),

-- we now process the all_cow_dao_solvers table so that we can associate the name of a solver under the CoW DAO bonding pool
-- with its "real" join time, i.e., the first time that it joined the pool (using potentially an account that changed on the way)
join_date_per_cow_dao_solver as (
    select
        solver_name,
        environment,
        min(evt_block_time) as join_time
    from all_cow_dao_solvers group by solver_name, environment
),

-- this is now the first calculation of whether a solver in the CoW DAO bonding pool needs to pay a service fee.
-- note that we assume here no solver has its own subpool, and we will correct that later on.
active_cow_dao_solvers_service_fee as (
    select
        c.solver_name,
        c.environment,
        a.solver_address,
        a.pool_name,
        case
            when c.join_time > date_add('day', -7, date_add('month', -6, cast('{{time}}' as timestamp))) then false
            else true
        end as service_fee_flag,
        c.join_time
    from join_date_per_cow_dao_solver as c inner join active_cow_dao_solver_names as a on c.solver_name = a.solver_name and c.environment = a.environment
),

reduced_bonds as (
    select * from "query_4065709"
)

select
    coalesce(e.solver_name, concat(environment, '-', d.solver_name)) as solver_name,
    coalesce(e.solver_address, d.solver_address) as solver,
    coalesce(e.pool_name, d.pool_name) as pool_name,
    case
        when e.creation_date > date_add('month', -3, cast('{{time}}' as timestamp)) then false
        else d.service_fee_flag
    end as service_fee
from active_cow_dao_solvers_service_fee as d left outer join reduced_bonds as e on d.solver_address = e.solver_address

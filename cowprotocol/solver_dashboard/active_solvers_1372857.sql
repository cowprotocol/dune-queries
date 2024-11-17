-- This query finds all the active (i.e. whitelisted) solvers
-- First we find the latest settlement for each solver
-- Then we fetch the environment and name of the solver for the prod and barn environments
-- Finally we display the name of the solver with both of its addresses
-- Note: if a solver has x barn address and y prod address, we will display x*y rows for that solver
-- Parameters
-- {{blockchain}}: string the blockchain to query

with
solver_latest_batches as (
    select
        solver_address,
        max(block_time) as latest_settlement
    from cow_protocol_{{blockchain}}.batches
    group by solver_address
),

active_solvers as (
    select
        address,
        environment,
        name,
        coalesce(latest_settlement, timestamp '1970-01-01') as latest_settlement
    from cow_protocol_{{blockchain}}.solvers
    full outer join solver_latest_batches
        on address = solver_address
    where
        environment not in ('test', 'service')
        and active = true
)

select
    prod.name,
    prod.address as prod_address,
    barn.address as barn_address,
    greatest(prod.latest_settlement, barn.latest_settlement) as latest_settlement
from active_solvers as prod
inner join active_solvers as barn
    on
        prod.name = barn.name
        and prod.environment = 'prod'
        and barn.environment = 'barn'
order by greatest(prod.latest_settlement, barn.latest_settlement) desc

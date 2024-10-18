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
    full outer join solver_latest_batches on address = solver_address
    where environment not in ('test', 'service')
    and active = true
)


select 
    prod.name as name,
    prod.address as prod_address,
    barn.address as barn_address,
    greatest(prod.latest_settlement, barn.latest_settlement) as latest_settlement
from active_solvers prod
join active_solvers barn
    on prod.name = barn.name
    and prod.environment = 'prod'
    and barn.environment = 'barn'
order by greatest(prod.latest_settlement, barn.latest_settlement) desc
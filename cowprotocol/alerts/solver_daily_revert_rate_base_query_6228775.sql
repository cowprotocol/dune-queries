with batch_data as (
    select
        a.solver,
        a.auction_id,
        case
            when a.tx_hash is null then 1
            when a.block_number is not null and a.block_number > a.block_deadline then 1
            else 0
        end as is_revert,
        b.time as deadline_timestamp
    from "query_4351957(blockchain='{{blockchain}}')" as a inner join {{blockchain}}.blocks as b
        on a.block_deadline = b.number
    where
        b.time >= now() - interval '1' day
),

breakdown_per_solver as (
    select
        solver,
        sum(is_revert) as total_num_reverts,
        count(*) as total_num_winning_solutions,
        100 * sum(is_revert) * 1.0000 / count(*) as percent_of_reverts
    from batch_data
    group by solver
),

solvers as (
    select
        address,
        environment,
        name,
        whitelisted as active
    from dune.cowprotocol.solvers
    where blockchain = '{{blockchain}}'
)

select
    s.environment,
    s.name,
    a.*
from breakdown_per_solver as a inner join solvers as s on a.solver = s.address
where a.total_num_winning_solutions > 20

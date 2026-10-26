-- CoW Protocol Solana solvers whitelisted status

with solver_names (solver, environment, name) as (
    values
    ('5GmuFPWYbVYD3jijMhtq5aGbhHi24ANqGcbFAirr284M', 'prod', 'Fractal'),
    ('6ZUeThQ9FovzaS8HejAfGW2VqCXwRYPKKyUNsozpfrSx', 'prod', 'Rizzolver')
),

events as (
    select
        solver,
        'add' as action,
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        coalesce(call_inner_instruction_index, 0) as call_inner_instruction_index,
        call_tx_id
    from cow_protocol_solana.cow_settlement_call_add_solver

    union all

    select
        solver,
        'remove' as action,
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        coalesce(call_inner_instruction_index, 0) as call_inner_instruction_index,
        call_tx_id
    from cow_protocol_solana.cow_settlement_call_remove_solver
),

ranked as (
    select
        *,
        row_number() over (
            partition by solver
            order by
                call_block_slot desc,
                call_tx_index desc,
                call_outer_instruction_index desc,
                call_inner_instruction_index desc
        ) as rn
    from events
)

select
    coalesce(n.environment, 'new') as environment,
    coalesce(n.name, 'Untitled') as name,
    r.solver,
    r.action                            as latest_action,
    r.action = 'add'                    as is_active,
    r.call_block_time                   as last_change_time,
    r.call_block_slot                   as last_change_slot,
    r.call_tx_index                     as last_change_tx_index,
    r.call_outer_instruction_index      as last_change_outer_ix_index,
    r.call_inner_instruction_index      as last_change_inner_ix_index,
    r.call_tx_id                        as last_change_tx
from ranked as r left join solver_names as n on r.solver = n.solver
where r.rn = 1
order by is_active desc, last_change_time desc

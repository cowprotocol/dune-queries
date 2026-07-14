-- Flags token approvals responsible to an address that is recognized as a solver
-- (i.e. not NON-SOLVER or PROPOSER-ACCOUNT) but that is either not currently
-- vouched for by a full bonding pool, or no longer whitelisted as an active
-- solver (e.g. deprecated/blacklisted solvers whose vouch was never revoked).
-- Parameters:
--  {{blockchain}} - network the query is run on
--  {{end_time}} - end date timestamp used to determine the current vouch status

with approvals as (
    select *
    from "query_4173928(blockchain='{{blockchain}}')"
),

vouched_solvers as (
    select distinct solver
    from "query_1541516(blockchain='{{blockchain}}',end_time='{{end_time}}',vouch_cte_name='valid_vouches')"
)

select
    approvals.*,
    approvals.responsible_address not in (select solver from vouched_solvers) as is_unvouched,
    not approvals.solver_whitelisted as is_deprecated,
    case
        when
            approvals.responsible_address not in (select solver from vouched_solvers)
            and not approvals.solver_whitelisted
            then 'unvouched_and_deprecated'
        when approvals.responsible_address not in (select solver from vouched_solvers) then 'unvouched'
        when not approvals.solver_whitelisted then 'deprecated'
    end as flag_reason
from approvals
where
    approvals.responsible_solver not in ('NON-SOLVER', 'PROPOSER-ACCOUNT')
    and (
        approvals.responsible_address not in (select solver from vouched_solvers)
        or not approvals.solver_whitelisted
    )
order by approvals.block_time desc

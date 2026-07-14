-- Flags token approvals responsible to an address that is recognized as a solver
-- (i.e. not NON-SOLVER or PROPOSER-ACCOUNT) but whose address is not currently
-- vouched for by a full bonding pool.
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

select *
from approvals
where
    responsible_solver not in ('NON-SOLVER', 'PROPOSER-ACCOUNT')
    and responsible_address not in (select solver from vouched_solvers)
order by block_time desc

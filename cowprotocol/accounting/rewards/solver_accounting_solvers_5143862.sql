-- Solver Accounting: Solvers
-- Solver addresses with their name, environment (prod / barn) and active flag for one chain.
-- Consumed by the weekly Dune -> analytics DB sync in cowprotocol/cow-dagster
-- (cow_dagster/analytics/solver_accounting/sync_solver_accounting_dune_data.yaml),
-- landing in dbt.dune_data__cow_protocol__solvers.
--
-- Parameters:
-- {{blockchain}}: the corresponding network

select
    address,
    environment,
    name,
    whitelisted as active
from dune.cowprotocol.solvers
where blockchain = '{{blockchain}}'

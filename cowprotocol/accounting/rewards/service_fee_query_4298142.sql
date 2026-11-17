-- This query returns which solvers need to pay service fee.
-- It reads query_5143916, the service fee tracker of the payout pipeline, so that both use the same logic.

-- Parameters:
-- {{start_time}}: the start date of an accounting week
-- {{end_time}}: the end date of an accounting week
-- {{blockchain}}: the corresponding network

select
    solver_name,
    solver,
    pool_name,
    service_fee
from "query_5143916(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"

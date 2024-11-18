-- This query computes all the rewards distributed to the solvers per week
-- The rewards are split into 3 categories: performance, consistency and quote
select
    week_start,
    sum(performance_reward) as performance_reward,
    sum(consistency_reward) as consistency_reward,
    sum(quote_reward) as quote_reward,
    sum(performance_reward + consistency_reward + coalesce(quote_reward, 0)) as total_reward
from query_3641173
group by week_start
order by week_start desc

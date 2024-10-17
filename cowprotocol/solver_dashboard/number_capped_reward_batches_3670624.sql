with
  batch_rewards as (
    select
      date_add('day', 1, date_trunc('week', date_add('day', -1, time))) as week_start,
      winning_solver,
      tx_hash,
      reward
    from
      query_2777544 bs
      join ethereum.blocks eb on bs.block_deadline = eb.number
    where
      time >= cast('2024-03-19 12:00:00' as timestamp) -- start of analysis with CIP-38
  ),
weekly_data as (
    select
        week_start,
        count(*) nr_auction,
        sum(case when tx_hash is not null then 1 else 0 end) nr_success,
        sum(case when tx_hash is null then 1 else 0 end) nr_fail,
        sum(case when reward = 12000000000000000 then 1 else 0 end) nr_capped_success,
        sum(case when reward = -10000000000000000 then 1 else 0 end) nr_capped_fail
    from batch_rewards
    group by
    week_start
)
select
    *,
    nr_success - nr_capped_success as nr_not_capped_success,
    nr_fail - nr_capped_fail as nr_not_capped_fail,
    nr_capped_success + nr_capped_fail as nr_capped,
    nr_auction - nr_capped_success - nr_capped_fail as nr_not_capped
from weekly_data


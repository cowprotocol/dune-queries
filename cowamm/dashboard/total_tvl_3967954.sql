-- Query computes the TVL over all CoW AMMs

with tvl as (
  select
    day,
    sum(value0 + value1) as tvl
  from dune.cowprotocol.result_amm_lp_infos
  where project = 'cow_amm'
  group by 1
)

select
    curr.day,
    prev.tvl as prev, 
    curr.tvl as curr,
    100*(curr.tvl-prev.tvl)/prev.tvl as growth
from tvl curr
join tvl prev
  on curr.day = prev.day + interval '7' day
order by day desc
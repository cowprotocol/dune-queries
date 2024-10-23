-- Query computes the TVL over all CoW AMMs
with prep as (
    select * from "query_4096107(blockchain='ethereum')"
    union
    select * from "query_4096107(blockchain='gnosis')"
    union
    select * from "query_4096107(blockchain='arbitrum')"
),

tvl as (
  select
    day,
    sum(tvl) as tvl
  from prep
  group by 1
)

select 
    prev.tvl as prev, 
    curr.tvl as curr,
    100*(curr.tvl-prev.tvl)/prev.tvl as growth
from tvl curr
join tvl prev
  on curr.day = prev.day + interval '7' day
  -- we don't have data for today
  and curr.day = date(now()) - interval '1' day
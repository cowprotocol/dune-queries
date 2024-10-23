-- Query computes the TVL over all CoW AMMs
with prep as (
    select * from "query_4096107(blockchain='ethereum')"
    union
    select * from "query_4096107(blockchain='gnosis')"
    union
    select * from "query_4096107(blockchain='arbitrum')"
)

select
  day,
  sum(tvl) as tvl
from prep
group by 1
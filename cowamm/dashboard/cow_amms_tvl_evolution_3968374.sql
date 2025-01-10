-- Query computes the TVL over all CoW AMMs
with prep as (
    select * from "query_4096107(blockchain='ethereum')"
    union all
    select * from "query_4096107(blockchain='gnosis')"
    union all
    select * from "query_4096107(blockchain='arbitrum')"
    union all
    select * from "query_4096107(blockchain='base')"
)

select
    day,
    sum(tvl) as tvl
from prep
group by 1
order by 1 desc

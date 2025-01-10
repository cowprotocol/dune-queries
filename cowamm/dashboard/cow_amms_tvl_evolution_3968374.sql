-- Query computes the TVL over all CoW AMMs

-- Parameters
-- {{aggregate_by}}: the frequence of the data, e.g. 'day', 'week', 'month'

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
    date_trunc('{{aggregate_by}}', day) as period,
    sum(tvl) as tvl
from prep
group by 1
order by 1 desc

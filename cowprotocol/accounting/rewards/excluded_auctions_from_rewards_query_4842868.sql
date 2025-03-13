with excluded_auctions as (
    select
        *,
        0 as multiplier
    from "query_4847222"
)

select
    bd.tx_hash,
    ea.multiplier
from excluded_auctions as ea inner join "query_4351957(blockchain='{{blockchain}}')" as bd on ea.environment = bd.environment and ea.auction_id = bd.auction_id

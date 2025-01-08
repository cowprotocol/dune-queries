-- This query returns the total protocol fees (per type) that CoW DAO accrued since inception

select
    sum(partner_fee_share) as partner_fee_share,
    sum(total_protocol_fee_in_eth) as total_protocol_fee_in_eth,
    sum(mev_blocker_fee_cow) as mev_blocker_fee_cow,
    sum(partner_fee_share) + sum(total_protocol_fee_in_eth) + sum(mev_blocker_fee_cow) as total_cow_dao_fee
from query_3700123

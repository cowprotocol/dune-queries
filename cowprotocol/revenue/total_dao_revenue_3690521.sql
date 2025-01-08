-- This query returns the total protocol fees (per type) that CoW DAO accrued since inception

select
    sum(total_partner_share_all_chains) as partner_fee_share,
    sum(total_protocol_fee_in_eth) as total_protocol_fee_in_eth,
    sum(mev_blocker) as mev_blocker_fee,
    sum(total_partner_share_all_chains) + sum(total_protocol_fee_in_eth) + sum(mev_blocker) as total_cow_dao_fee
from query_3700123

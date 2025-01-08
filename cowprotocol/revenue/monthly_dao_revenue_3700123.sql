-- This query returns the protocol fees (per type) that CoW DAO accrues per month

with cow_mainnet as (
    select
        date_trunc('month', block_time) as date_month,
        coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0) as protocol_fee,
        coalesce(sum("Partner Fee Share"), 0) as partner_fee_share
    from "query_4217030(blockchain='ethereum',ui_fee_recipient='0x0000000000000000000000000000000000000000')"
    group by 1
),

cow_gnosis as (
    select
        date_trunc('month', block_time) as date_month,
        (coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0)) as protocol_fee,
        coalesce(sum("Partner Fee Share"), 0) as partner_fee_share
    from "query_4217030(blockchain='arbitrum',ui_fee_recipient='0x6b3214fD11dc91De14718DeE98Ef59bCbFcfB432')"
    group by 1
),

cow_arbitrum as (
    select
        date_trunc('month', block_time) as date_month,
        coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0) as protocol_fee,
        coalesce(sum("Partner Fee Share"), 0) as partner_fee_share
    from "query_4217030(blockchain='arbitrum',ui_fee_recipient='0x451100Ffc88884bde4ce87adC8bB6c7Df7fACccd')"
    group by 1
),

mevblocker as (
    select
        date_trunc('month', call_block_time) as date_month,
        sum(t.due / 1e18 / 2) as mev_blocker_fee
    from mev_blocker_ethereum.MevBlockerFeeTill_call_bill
    cross join unnest(due) as t (due)
    where
        call_success = true
    group by 1
)

select
    cm.date_month as "month",
    cm.protocol_fee as "Protocol (Mainnet)",
    ca.protocol_fee as "Protocol (Arbitrum)",
    cg.protocol_fee as "Protocol (Gnosis)",
    mev_blocker_fee as mev_blocker,
    coalesce(cm.partner_fee_share, 0) + coalesce(ca.partner_fee_share, 0) + coalesce(cg.partner_fee_share, 0) as total_partner_share_all_chains,
    coalesce(cm.protocol_fee, 0) + coalesce(ca.protocol_fee, 0) + coalesce(cg.protocol_fee, 0) as "total_protocol_fee_in_eth"
from cow_mainnet as cm
left join cow_arbitrum as ca
    on cm.date_month = ca.date_month
left join cow_gnosis as cg
    on cm.date_month = cg.date_month
left join mevblocker as m
    on cm.date_month = m.date_month
where cm.date_month >= date '2024-01-01'

-- This query returns the protocol fees (per type) that CoW DAO accrues per month

with cow_mainnet as (
    select
        date_trunc('month', block_time) as date_month,
        coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0) as protocol_fee,
        coalesce(sum("Partner Fee Share"), 0) as partner_fee_share
    from "query_4217030(blockchain='ethereum',ui_fee_recipient='0x0000000000000000000000000000000000000000')"
    group by 1
),

daily_eth_price as (
    select
        day,
        price
    from prices.usd_daily
    where
        blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
),

cow_gnosis as (
    select
        date_trunc('month', block_time) as date_month,
        (coalesce(sum("Limit" / price), 0) + coalesce(sum("Market" / price), 0) + coalesce(sum("UI Fee" / price), 0)) as protocol_fee,
        coalesce(sum("Partner Fee Share" / price), 0) as partner_fee_share
    from "query_4217030(blockchain='gnosis',ui_fee_recipient='0x6b3214fD11dc91De14718DeE98Ef59bCbFcfB432')"
    left join daily_eth_price on day = date_trunc('day', block_time)
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

cow_base as (
    select
        date_trunc('month', block_time) as date_month,
        coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0) as protocol_fee,
        coalesce(sum("Partner Fee Share"), 0) as partner_fee_share
    from "query_4217030(blockchain='base',ui_fee_recipient='0x0000000000000000000000000000000000000000')"
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
    cb.protocol_fee as "Protocol (Base)",
    mev_blocker_fee as mev_blocker,
    coalesce(cm.partner_fee_share, 0) + coalesce(ca.partner_fee_share, 0) + coalesce(cg.partner_fee_share, 0) + coalesce(cb.partner_fee_share, 0) as total_partner_share_all_chains,
    coalesce(cm.protocol_fee, 0) + coalesce(ca.protocol_fee, 0) + coalesce(cg.protocol_fee, 0) + coalesce(cb.protocol_fee, 0) as "total_protocol_fee_in_eth"
from cow_mainnet as cm
left join cow_arbitrum as ca
    on cm.date_month = ca.date_month
left join cow_gnosis as cg
    on cm.date_month = cg.date_month
left join cow_base as cb
    on cm.date_month = cb.date_month
left join mevblocker as m
    on cm.date_month = m.date_month
where cm.date_month >= date '2024-01-01'

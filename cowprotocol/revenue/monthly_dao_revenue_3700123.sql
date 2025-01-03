with
cow_fee as (
    select
        date_trunc('month', block_date) as date_month,
        sum(f.protocol_fee * f.protocol_fee_native_price / pow(10, 18))
        - coalesce(sum(case when f.partner_fee_recipient is not null then f.partner_fee * f.protocol_fee_native_price / pow(10, 18) end), 0) as total_protocol_fee_in_eth,
        -- partner fee is calculated based on 15% cut that goes to cow dao
        sum(case when f.partner_fee_recipient is not null then f.partner_fee * f.protocol_fee_native_price / pow(10, 18) end) as partner_fee_eth,
        sum(case when f.partner_fee_recipient is not null then f.partner_fee * f.protocol_fee_native_price / pow(10, 18) * cast(0.15 as double) end) as partner_fee_share
    from cow_protocol_ethereum.trades as t
    left join "query_4364122(blockchain='ethereum')" as f
        on
            t.order_uid = f.order_uid
            and t.tx_hash = f.tx_hash
            -- rough block around which the DAO started accruing fees
            and f.block_number > 19068880
            and f.protocol_fee_native_price > 0
    where
        t.block_number > 19068880
        -- some orders report unrealistic fees due to incorrect native prices
        and t.order_uid not in (select order_uid from query_3639473)
    group by 1
),

mev_fee as (
    select
        date_trunc('month', call_block_time) as date_month,
        sum(t.due / 1e18 / 2) as mev_blocker_fee_cow
    from mev_blocker_ethereum.MevBlockerFeeTill_call_bill
    cross join unnest(due) as t (due)
    where
        call_success = true
    group by 1
)

select
    c.date_month as "month",
    partner_fee_share,
    total_protocol_fee_in_eth,
    mev_blocker_fee_cow,
    coalesce(partner_fee_share, 0) + coalesce(total_protocol_fee_in_eth, 0) + coalesce(mev_blocker_fee_cow, 0) as total_cow_dao_fee
from cow_fee as c
left join mev_fee as m
    on c.date_month = m.date_month

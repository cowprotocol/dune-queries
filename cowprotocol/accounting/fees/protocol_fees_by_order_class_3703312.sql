-- This query computes protocol fees collected by the DAO,
-- and breaks down the revenue based on order class.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)

with
protocol_fees_collected as (
    select --noqa: ST06
        order_uid, --noqa: RF03
        cast(cast(data.protocol_fee as varchar) as int256) * data.protocol_fee_native_price / pow(10, 18) as protocol_fee_in_eth, --noqa: RF01, RF03
        cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee_in_surplus_token, --noqa: RF01
        case
            when data.partner_fee_recipient is not null then cast(data.partner_fee as int256) * data.protocol_fee_native_price / pow(10, 18) --noqa: RF01
        end as partner_fee_eth,
        cast(cast(data.protocol_fee as varchar) as int256) * data.protocol_fee_native_price / pow(10, 18) - coalesce(case when data.partner_fee_recipient is not null then cast(data.partner_fee as int256) * data.protocol_fee_native_price / pow(10, 18) end, 0) as net_protocol_fee_in_eth, --noqa: RF01
        data.protocol_fee_token as surplus_token, --noqa: RF01
        data.quote_gas_cost, --noqa: RF01
        data.quote_sell_token_price, --noqa: RF01
        data.quote_sell_amount, --noqa: RF01
        data.quote_buy_amount, --noqa: RF01
        tx_hash, --noqa: RF03
        data.protocol_fee_kind --noqa: RF01
    from cowswap.raw_order_rewards
    where block_number > 19068880 and order_uid not in (select cast(order_uid as varchar) from query_3639473) --noqa: RF03
    -- context: CoW DAO enabled protocol fees after mainnet block 19068880; there were no protocol fees collected up till that block.
)
select
    order_class,
    sum(protocol_fee_in_eth) as total_fee_in_eth,
    sum(net_protocol_fee_in_eth) as net_protocol_fee_in_eth,
    sum(partner_fee_eth) as total_partner_fee,
    sum(case
        when partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_eth * 0.90
        when partner_recipient is not null then partner_fee_eth * 0.85
    end) as partner_fee_part,
    sum(case
        when partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_eth * 0.10
        when partner_recipient is not null then partner_fee_eth * 0.15
    end) as cow_dao_partner_fee_part
from protocol_fees_collected as f
inner join cow_protocol_ethereum.trades as t
    on f.order_uid = cast(t.order_uid as varchar) and f.tx_hash = cast(t.tx_hash as varchar)
left join dune.cowprotocol.result_cow_protocol_ethereum_app_data as a on t.app_data = a.app_hash
where
    block_date >= date '2024-01-23'
    and block_date >= timestamp'{{start_time}}'
    and block_date < timestamp'{{end_time}}'
group by 1
order by 1 desc

-- This query computes protocol fees collected by the DAO,
-- and breaks down the revenue based on order class.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)

with
block_range as (
    select * from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

-- context: CoW DAO enabled protocol fees after mainnet block 19068880; there were no protocol fees collected up till that block.
initial_block as (
    select
        case
            when '{{blockchain}}' = 'ethereum' then 19068880
            else 1
        end as initial_block
),

protocol_fees_collected as (
    select --noqa: ST06
        order_uid,
        protocol_fee * protocol_fee_native_price / pow(10, 18) as protocol_fee_in_eth,
        protocol_fee as protocol_fee_in_surplus_token,
        case
            when partner_fee_recipient is not null then partner_fee * protocol_fee_native_price / pow(10, 18)
        end as partner_fee_eth,
        protocol_fee * protocol_fee_native_price / pow(10, 18) - coalesce(case when partner_fee_recipient is not null then cast(partner_fee as int256) * protocol_fee_native_price / pow(10, 18) end, 0) as net_protocol_fee_in_eth,
        protocol_fee_token as surplus_token,
        quote_gas_cost,
        quote_sell_token_price,
        quote_sell_amount,
        quote_buy_amount,
        tx_hash
    from "query_4364122(blockchain='{{blockchain}}')"
    where block_number > (select initial_block from initial_block) and cast(order_uid as varchar) not in (select order_uid from query_3639473)
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
inner join cow_protocol_{{blockchain}}.trades as t
    on f.order_uid = t.order_uid and f.tx_hash = t.tx_hash
left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as a on t.app_data = a.app_hash
where
    block_number >= (select start_block from block_range)
    and block_number < (select end_block from block_range)
group by 1
order by 1 desc

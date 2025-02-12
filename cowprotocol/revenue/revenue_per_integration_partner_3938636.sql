with
protocol_fees_collected as (
    select --noqa: ST06
        order_uid,
        protocol_fee * protocol_fee_native_price / pow(10, 18) as protocol_fee_in_eth,
        case when partner_fee_recipient is not null then partner_fee * protocol_fee_native_price / pow(10, 18) end as partner_fee_eth,
        protocol_fee * protocol_fee_native_price / pow(10, 18) - coalesce(case when partner_fee_recipient is not null then partner_fee * protocol_fee_native_price / pow(10, 18) end, 0) as net_protocol_fee_in_eth,
        protocol_fee as protocol_fee_in_surplus_token,
        protocol_fee_token as surplus_token,
        quote_gas_cost,
        quote_sell_token_price,
        quote_sell_amount,
        quote_buy_amount,
        tx_hash,
        protocol_fee_kind
    from "query_4364122(blockchain='ethereum')"
    where block_number > 19068880 and order_uid not in (select order_uid from "query_3639473")
)

select
    date_trunc('{{aggregate_by}}', block_date) as block_date,
    app_code,
    count(f.order_uid) as cnt_orders,
    count(distinct f.tx_hash) as cnt_tx,
    sum(protocol_fee_in_eth) as protocol_fee_in_eth,
    sum(net_protocol_fee_in_eth) as net_protocol_fee_in_eth,
    sum(net_protocol_fee_in_eth) / count(f.order_uid) as fee_per_order,
    avg(net_protocol_fee_in_eth) as avg_net_protocol_fee_in_eth,
    approx_percentile(net_protocol_fee_in_eth, 0.5) as med_net_protocol_fee_in_eth
from cow_protocol_ethereum.trades as t --noqa: ST09
left join protocol_fees_collected as f
    on t.order_uid = f.order_uid and t.tx_hash = f.tx_hash
left join dune.cowprotocol.result_cow_protocol_ethereum_app_data as a
    on a.app_hash = t.app_data
where block_date >= date '2024-01-23' and block_number > 19068880
group by 1, 2
order by 1 desc

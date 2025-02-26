-- This query returns a list of trades (one record for each order per settlement) and lists the different fees the trade incurred
-- Parameters:
--  {{ui_fee_recipient}} - the partner address that receives the CoW Swap UI fee
--  {{blockchain}} - the chain for which to collect the data

select
    t.block_time,
    protocol_fee / pow(10, 18) * cast(protocol_fee_native_price as double) as "Total (including ext. Partner Fee)",
    if(t.block_number < 19564399 or protocol_fee_kind = 'surplus', protocol_fee - coalesce(partner_fee, 0)) / pow(10, 18) * cast(protocol_fee_native_price as double) as "Limit",
    if(protocol_fee_kind = 'priceimprovement', protocol_fee - coalesce(partner_fee, 0)) / pow(10, 18) * cast(protocol_fee_native_price as double) as "Market",
    if(partner_fee_recipient in ({{ui_fee_recipient}}), partner_fee / pow(10, 18) * cast(protocol_fee_native_price as double)) as "UI Fee",
    if(partner_fee_recipient not in ({{ui_fee_recipient}}),
    -- some partners have custom fee shares
    case partner_fee_recipient
        when 0x63695eee2c3141bde314c5a6f89b98e62808d716 then 0.1
        else 0.15
    end * partner_fee / pow(10, 18) * cast(protocol_fee_native_price as double)) as "Partner Fee Share",
    d.app_code,
    t.usd_value,
    t.order_uid,
    t.tx_hash,
    r.solver
from "query_4364122(blockchain='{{blockchain}}')" as r
inner join cow_protocol_{{blockchain}}.trades as t
    on
        r.order_uid = t.order_uid
        and r.tx_hash = t.tx_hash
left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as d
    on t.app_data = d.app_hash
where t.order_uid not in (select order_uid from query_3639473)
order by block_time desc

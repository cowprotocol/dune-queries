with
per_settlement_prelim as (
    select
        t.block_time,
        t.order_uid,
        t.tx_hash,
        r.data.partner_fee_recipient as partner_recipient,
        t.app_data,
        usd_value,
        cast(cast(r.data.protocol_fee as varchar) as int256) as protocol_fee,
        r.data.protocol_fee_token,
        json_extract(a.encode, '$.metadata.partnerFee.bps') as patnerFeeBps,
        json_extract(a.encode, '$.metadata.widget.appCode') as app_code,
        cast(
            usd_value * cast(
                json_extract(a.encode, '$.metadata.partnerFee.bps') as double
            ) as double
        ) / 10000 as est_partner_revenue,
        cast(
            usd_value * cast(
                json_extract(a.encode, '$.metadata.partnerFee.bps') as double
            ) as double
        ) / 10000 * 0.15 as est_cow_revenue,
        cast(
            cast(
                coalesce(r.data.partner_fee, r.data.protocol_fee) as varchar
            ) as int256
        ) * r.data.protocol_fee_native_price / pow(10, 18) as raw_integrator_fee_in_eth
    from
        cow_protocol_ethereum.trades as t
    left join dune.cowprotocol.dataset_app_data_mainnet as a on t.app_data = a.contract_app_data
    left join cowswap.raw_order_rewards as r
        on
            r.order_uid = cast(t.order_uid as varchar)
            and t.tx_hash = from_hex(r.tx_hash)
    where
        json_extract(a.encode, '$.metadata.partnerFee.recipient') is not null
        and t.block_time >= cast('{{start_time}}' as timestamp)
        and t.block_time < cast('{{end_time}}' as timestamp)
    order by
        t.block_time desc
),

per_settlement as (
    select *
    from
        per_settlement_prelim
    where
        raw_integrator_fee_in_eth > 0
),

aggregate_per_recipient as (
    select
        partner_recipient,
        case
            when partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then sum(0.9 * raw_integrator_fee_in_eth)
            else sum(0.85 * raw_integrator_fee_in_eth)
        end as partner_fee_part,
        case
            when partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then sum(0.1 * raw_integrator_fee_in_eth)
            else sum(0.15 * raw_integrator_fee_in_eth)
        end as cow_dao_partner_fee_part
    from
        per_settlement
    group by
        partner_recipient
)

select * from {{result}}

-- This query computes the partner fees associated with each partner in a given time interval
--
-- Parameters:
--  {{start_time}} - the timestamp for which the accounting should start (inclusively)
--  {{end_time}} - the timestamp for which the accounting should end (exclusively)
--  {{result}} - two views of the result, one aggregated and one on a per tx basis
--  {{blockchain}} - the blockchain for which to fetch the data

with
per_trade_protocol_fees as (
    select
        t.block_time,
        t.block_number,
        t.order_uid,
        t.tx_hash,
        r.partner_fee_recipient as partner_recipient,  -- noqa: RF01
        usd_value,
        protocol_fee,  -- noqa: RF01
        r.protocol_fee_token,  -- noqa: RF01
        a.partner_bps,
        a.widget_app_code,
        a.app_code,
        usd_value * cast(
            a.partner_bps as double
        ) / 10000 as est_partner_revenue,
        usd_value * cast(
            a.partner_bps as double
        ) / 10000 * 0.15 as est_cow_revenue,
        cast(
            cast(
                coalesce(r.partner_fee, r.protocol_fee) as varchar  -- noqa: RF01
            ) as int256
        ) * r.protocol_fee_native_price / pow(10, 18) as raw_integrator_fee_in_eth  -- noqa: RF01
    from
        cow_protocol_{{blockchain}}.trades as t
    left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as a on t.app_data = a.app_hash
    left join "query_4364122(blockchain='{{blockchain}}')" as r
        on
            r.order_uid = t.order_uid
            and r.tx_hash = t.tx_hash
    where
        a.partner_recipient is not null
        and t.block_number >= (select start_block from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')")
        and t.block_number < (select end_block from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')")
    order by
        t.block_time desc
),

per_trade_partner_fees as (
    select *
    from
        per_trade_protocol_fees
    where
        raw_integrator_fee_in_eth > 0
),

per_recipient_partner_fees as (
    select
        partner_recipient,
        app_code,
        widget_app_code,
        case
            -- mainnet
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x90a48d5cf7343b08da12e067680b4c6dbfe551be then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            -- gnosis
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x8387fae9951724c00c753797b22b897111750673 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xb0E3175341794D1dc8E5F02a02F9D26989EbedB3 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            -- arbitrum
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x86cd2bBC859E797B75D86E6eEEC1a726A9284c23 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x38276553F8fbf2A027D901F8be45f00373d8Dd48 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            -- base
            when '{{blockchain}}' = 'base' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xAf1c727B605530AcDb00906a158E817f41aFD778 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            else sum(0.5 * raw_integrator_fee_in_eth)
        end as partner_fee_part,
        case
            -- mainnet
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.1 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x90a48d5cf7343b08da12e067680b4c6dbfe551be then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.15 * raw_integrator_fee_in_eth)
            -- gnosis
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.1 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x8387fae9951724c00c753797b22b897111750673 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xb0E3175341794D1dc8E5F02a02F9D26989EbedB3 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.15 * raw_integrator_fee_in_eth)
            -- arbitrum
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.1 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x86cd2bBC859E797B75D86E6eEEC1a726A9284c23 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x38276553F8fbf2A027D901F8be45f00373d8Dd48 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.15 * raw_integrator_fee_in_eth)
            -- base
            when '{{blockchain}}' = 'base' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.1 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xAf1c727B605530AcDb00906a158E817f41aFD778 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502 then sum(0.15 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.15 * raw_integrator_fee_in_eth)
            else sum(0.5 * raw_integrator_fee_in_eth)
        end as cow_dao_partner_fee_part
    from
        per_trade_partner_fees
    group by
        partner_recipient, app_code, widget_app_code
)

select * from {{result}}

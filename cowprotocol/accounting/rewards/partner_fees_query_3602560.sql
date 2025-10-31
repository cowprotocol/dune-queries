-- This query computes the partner fees associated with each partner in a given time interval
--
-- Parameters:
--  {{start_time}} - the timestamp for which the accounting should start (inclusively)
--  {{end_time}} - the timestamp for which the accounting should end (exclusively)
--  {{result}} - two views of the result, one aggregated and one on a per tx basis
--  {{blockchain}} - the blockchain for which to fetch the data

with per_trade_protocol_fees as (
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
        usd_value * cast(a.partner_bps as double) / 10000 as est_partner_revenue,
        usd_value * cast(a.partner_bps as double) / 10000 * 0.15 as est_cow_revenue,
        cast(cast(coalesce(r.partner_fee, r.protocol_fee) as varchar) as int256) * r.protocol_fee_native_price / pow(10, 18) as raw_integrator_fee_in_eth --noqa: LT02
    from cow_protocol_{{blockchain}}.trades as t
    left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as a on t.app_data = a.app_hash
    left join "query_4364122(blockchain='{{blockchain}}')" as r
        on
        t.order_uid = r.order_uid
        and t.tx_hash = r.tx_hash
    where
        a.partner_recipient is not null
        and t.block_number >= (select start_block from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')")
        and t.block_number < (select end_block from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')")
    order by t.block_time desc
),

per_trade_partner_fees_prelim as (
    select *
    from
        per_trade_protocol_fees
    where
        raw_integrator_fee_in_eth > 0
),

per_trade_partner_fees as (
    select
        block_time,
        block_number,
        order_uid,
        tx_hash,
        case
            when partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' and block_time >= cast('2025-08-26 00:00:00' as timestamp) then 0xe344241493d573428076c022835856a221db3e26
            else partner_recipient
        end as partner_recipient,  -- noqa: RF01
        usd_value,
        protocol_fee,  -- noqa: RF01
        protocol_fee_token,  -- noqa: RF01
        partner_bps,
        widget_app_code,
        app_code,
        est_partner_revenue,  
        est_cow_revenue,
        raw_integrator_fee_in_eth
    from per_trade_partner_fees_prelim
),


per_recipient_partner_fees_prelim as (
    select
        partner_recipient,
        app_code,
        widget_app_code,
        sum(raw_integrator_fee_in_eth) as total_raw_amount,
        case
            -- mainnet
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x90a48d5cf7343b08da12e067680b4c6dbfe551be then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'ethereum' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- gnosis
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x8387fae9951724c00c753797b22b897111750673 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xb0e3175341794d1dc8e5f02a02f9d26989ebedb3 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'gnosis' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- arbitrum
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x86cd2bbc859e797b75d86e6eeec1a726a9284c23 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x38276553f8fbf2a027d901f8be45f00373d8dd48 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'arbitrum' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- base
            when '{{blockchain}}' = 'base' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xaf1c727b605530acdb00906a158e817f41afd778 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x9c9aa90363630d4ab1d9dbf416cc3bbc8d3ed502 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'base' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- avalanche_c
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0xaf1c727b605530acdb00906a158e817f41afd778 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0x9c9aa90363630d4ab1d9dbf416cc3bbc8d3ed502 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'avalanche_c' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- polygon
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0xaf1c727b605530acdb00906a158e817f41afd778 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0x9c9aa90363630d4ab1d9dbf416cc3bbc8d3ed502 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'polygon' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- bnb
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0x63695eee2c3141bde314c5a6f89b98e62808d716 and app_code != 'CoW Swap-SafeApp' then sum(0.9 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0x352a3666b27bb09aca7b4a71ed624429b7549551 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0xaf1c727b605530acdb00906a158e817f41afd778 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0x9c9aa90363630d4ab1d9dbf416cc3bbc8d3ed502 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0xe344241493d573428076c022835856a221db3e26 then sum(0.85 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0x8025bacf968aa82bdfe51b513123b55bfb0060d3 then sum(0.45 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0xe423c63e8a25811c9cbe71c8585c4505117397c6 then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb then sum(0.75 * raw_integrator_fee_in_eth)
            when '{{blockchain}}' = 'bnb' and partner_recipient = 0xc542c2f197c4939154017c802b0583c596438380 then sum(0.875 * raw_integrator_fee_in_eth)
            -- default
            else sum(0.5 * raw_integrator_fee_in_eth)
        end as partner_fee_part
    from
        per_trade_partner_fees
    group by
        partner_recipient, app_code, widget_app_code
),

per_recipient_partner_fees as (
    select
        partner_recipient,
        app_code,
        widget_app_code,
        partner_fee_part,
        total_raw_amount - partner_fee_part as cow_dao_partner_fee_part
    from per_recipient_partner_fees_prelim
)

select * from {{result}}

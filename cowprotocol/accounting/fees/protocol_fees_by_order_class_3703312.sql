-- This query computes protocol fees collected by the DAO,
-- and breaks down the revenue based on order class.
-- Native token is ETH, with the exception of Gnosis Chain, where it is xDAI
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}}: the corresponding network

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
        protocol_fee * protocol_fee_native_price / pow(10, 18) as protocol_fee_in_native_token,
        protocol_fee as protocol_fee_in_surplus_token,
        case
            when partner_fee_recipient is not null then partner_fee * protocol_fee_native_price / pow(10, 18)
        end as partner_fee_native_token,
        protocol_fee * protocol_fee_native_price / pow(10, 18) - coalesce(case when partner_fee_recipient is not null then cast(partner_fee as int256) * protocol_fee_native_price / pow(10, 18) end, 0) as net_protocol_fee_in_native_token, --noqa: AL03, PRS
        protocol_fee_token as surplus_token,
        quote_gas_cost,
        quote_sell_token_price,
        quote_sell_amount,
        quote_buy_amount,
        tx_hash
    from "query_4364122(blockchain='{{blockchain}}')"
    where block_number > (select initial_block from initial_block) and order_uid not in (select order_uid from query_3639473)
)

select
    order_class,
    sum(protocol_fee_in_native_token) as total_fee_in_native_token,
    sum(net_protocol_fee_in_native_token) as net_protocol_fee_in_native_token,
    sum(partner_fee_native_token) as total_partner_fee,
    sum(case
        -- mainnet
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x90a48d5cf7343b08da12e067680b4c6dbfe551be' then partner_fee_native_token * 0.85
        -- gnosis
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x8387fae9951724c00c753797b22b897111750673' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0xb0E3175341794D1dc8E5F02a02F9D26989EbedB3' then partner_fee_native_token * 0.85
        -- arbitrum
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x86cd2bBC859E797B75D86E6eEEC1a726A9284c23' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x38276553F8fbf2A027D901F8be45f00373d8Dd48' then partner_fee_native_token * 0.85
        -- base
        when '{{blockchain}}' = 'base' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'base' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'base' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'base' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.85
        -- avalanche_c
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.85
        -- polygon
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.85
        -- bnb
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.85
        -- linea
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.90
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'linea' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.85
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.85
        -- default
        when partner_recipient is not null then partner_fee_native_token * 0.75
    end) as partner_fee_part,
    sum(case
        -- mainnet
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'ethereum' and partner_recipient = '0x90a48d5cf7343b08da12e067680b4c6dbfe551be' then partner_fee_native_token * 0.15
        -- gnosis
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0x8387fae9951724c00c753797b22b897111750673' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'gnosis' and partner_recipient = '0xb0E3175341794D1dc8E5F02a02F9D26989EbedB3' then partner_fee_native_token * 0.15
        -- arbitrum
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x86cd2bBC859E797B75D86E6eEEC1a726A9284c23' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'arbitrum' and partner_recipient = '0x38276553F8fbf2A027D901F8be45f00373d8Dd48' then partner_fee_native_token * 0.15
        -- base
        when '{{blockchain}}' = 'base' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'base' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'base' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'base' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.15
        -- avalanche_c
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'avalanche_c' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.15
        -- polygon
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'polygon' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.15
        -- bnb
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'bnb' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.15
        -- linea
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x63695Eee2c3141BDE314C5a6f89B98E62808d716' then partner_fee_native_token * 0.10
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x352a3666b27bb09aca7b4a71ed624429b7549551' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'linea' and partner_recipient = '0xAf1c727B605530AcDb00906a158E817f41aFD778' then partner_fee_native_token * 0.15
        when '{{blockchain}}' = 'linea' and partner_recipient = '0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502' then partner_fee_native_token * 0.15
        -- default
        when partner_recipient is not null then partner_fee_native_token * 0.25
    end) as cow_dao_partner_fee_part
from protocol_fees_collected as f
inner join cow_protocol_{{blockchain}}.trades as t
    on f.order_uid = t.order_uid and f.tx_hash = t.tx_hash
left join dune.cowprotocol.result_cow_protocol_{{blockchain}}_app_data as a on t.app_data = a.app_hash
where
    block_number >= (select start_block from block_range)
    and block_number <= (select end_block from block_range)
group by 1
order by 1 desc

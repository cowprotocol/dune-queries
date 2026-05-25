-- Bogus native prices alert (sell orders only)
-- Flags settled SELL orders where the buy-side and sell-side native values diverge
-- by >1000x, indicating that at least one of the two native prices used by the
-- protocol is wrong. A volume floor (min_native_volume) avoids firing on dust orders.
--
-- For SELL orders, protocol_fee_token = buy_token, so:
--   sell_token_native_price = rod.quote_sell_token_price
--   buy_token_native_price  = rod.protocol_fee_native_price
-- (Same convention used in rewards_per_auction_5787562.sql.)
--
-- Parameters:
--   blockchain         : ethereum (v1 mainnet-only)
--   start_time, end_time : 24h window injected by Dagster
--   min_native_volume  : volume floor in chain-native units (e.g. 0.01 = 0.01 ETH)

with sell_orders as (
    select
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.order_uid,
        rod.solver,
        t.sell_token_address,
        t.buy_token_address,
        t.atoms_sold,
        t.atoms_bought,
        rod.quote_sell_token_price as sell_token_native_price,
        rod.protocol_fee_native_price as buy_token_native_price,
        cast(t.atoms_sold as double) * rod.quote_sell_token_price / 1e18 as sell_value_native,
        cast(t.atoms_bought as double) * rod.protocol_fee_native_price / 1e18 as buy_value_native
    from cow_protocol_{{blockchain}}.trades as t
    inner join "query_4364122(blockchain='{{blockchain}}')" as rod
        on t.order_uid = rod.order_uid
        and t.tx_hash = rod.tx_hash
    where t.order_type = 'SELL'
        and t.block_time >= timestamp '{{start_time}}'
        and t.block_time < timestamp '{{end_time}}'
        and rod.quote_sell_token_price is not null
        and rod.protocol_fee_native_price is not null
        and rod.quote_sell_token_price > 0
        and rod.protocol_fee_native_price > 0
)

select
    block_time,
    order_uid,
    tx_hash,
    solver,
    sell_token_address,
    buy_token_address,
    atoms_sold,
    atoms_bought,
    sell_token_native_price,
    buy_token_native_price,
    sell_value_native,
    buy_value_native,
    case
        when sell_value_native > 1000 * buy_value_native then 'sell_side_overpriced'
        when buy_value_native > 1000 * sell_value_native then 'buy_side_overpriced'
    end as anomaly_type
from sell_orders
where (sell_value_native > 1000 * buy_value_native
    or buy_value_native > 1000 * sell_value_native
)
    and greatest(sell_value_native, buy_value_native) > {{min_native_volume}}
order by block_time desc

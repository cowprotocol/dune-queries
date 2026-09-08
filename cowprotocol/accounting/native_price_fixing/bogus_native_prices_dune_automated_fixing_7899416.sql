-- This query automatically detects issues with native prices
-- and proposes suggested fixes for them. The output produced
-- is a csv that is ready to be plugged into dbt as a seed
-- to correct flagged prices.
-- Parameters:
--   blockchain: the chain for which we want to retrieve batch data

-- The output has the following columns:
--    blockchain: varchar
--    environment: varchar
--    auction_id: integer
--    token: varbinary
--    price: decimal(38, 0)

with block_range as (
    select
        min("number") as start_block,
        max("number") as end_block
    from {{blockchain}}.blocks
    where
        time >= cast('{{start_time}}' as timestamp)
        and
        time < cast('{{end_time}}' as timestamp)
),

all_trades as (
    select
        t.block_time,
        date_trunc('hour', t.block_time) as hour, --noqa: RF04
        rd.auction_id,
        rd.environment,
        t.order_uid,
        t.order_type,
        t.tx_hash,
        t.sell_token_address,
        t.buy_token_address,
        t.usd_value,
        t.atoms_sold,
        t.atoms_bought,
        rd.protocol_fee,
        rd.protocol_fee_native_price,
        rd.protocol_fee * rd.protocol_fee_native_price / pow(10,18) as protocol_fee_collected_native_units
    from cow_protocol_{{blockchain}}.trades as t inner join "query_4364122(blockchain='{{blockchain}}')" as rd
        on t.order_uid = rd.order_uid and t.tx_hash = rd.tx_hash
    where t.block_number >= (select start_block from block_range) and t.block_number <= (select end_block from block_range)
),

prices as (
    select *
    from "query_4064601(blockchain='{{blockchain}}', start_time='{{start_time}}',end_time='{{end_time}}')"
),

trades_with_dune_prices as (
    select
        t.*,
        sp.price_atom as sell_token_atom_usd_price,
        sp.price_unit as sell_token_unit_usd_price,
        bp.price_atom as buy_token_atom_usd_price,
        bp.price_unit as buy_token_unit_usd_price,
        np.price_atom as native_token_atom_usd_price,
        np.price_unit as native_token_unit_usd_price
    from all_trades as t
    left join prices as sp on t.hour = sp.hour and t.sell_token_address = sp.token_address
    left join prices as bp on t.hour = bp.hour and t.buy_token_address = bp.token_address
    left join prices as np on t.hour = np.hour and np.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
),

-- here we flag trades where two conditions are both satisfied:
--   1. protocol fees are more than 5% of the trade
--   2. total protocol fees are more than {{protocol_fee_usd_threshold}} in absolute value
problematic_trades as (
    select
        *,
        protocol_fee_collected_native_units * native_token_unit_usd_price as protocol_fee_collected_usd
    from trades_with_dune_prices
    where
        protocol_fee_collected_native_units * native_token_unit_usd_price / usd_value > {{protocol_fee_relative_threshold}}
        and
        protocol_fee_collected_native_units * native_token_unit_usd_price > {{protocol_fee_usd_threshold}}

),

ready_to_use as (
    select
        case
            when '{{blockchain}}' = 'ethereum' then 'mainnet'
            when '{{blockchain}}' = 'avalanche_c' then 'avalanche'
            when '{{blockchain}}' = 'gnosis' then 'xdai'
            else '{{blockchain}}'
        end as blockchain,
        environment,
        auction_id,
        case
            when order_type = 'SELL' then buy_token_address
            else sell_token_address
        end as token,
        case
            when order_type = 'SELL' then cast(floor((buy_token_atom_usd_price/native_token_atom_usd_price) * pow(10, 18)) as decimal(38,0))
            when order_type = 'BUY' then cast(floor((sell_token_atom_usd_price/native_token_atom_usd_price) * pow(10, 18)) as decimal(38,0))
        end as price
    from problematic_trades
)

select * from ready_to_use

-- This query provides data related to rewards/payouts on a per batch auction level
-- for all auctions that had at least one winner.
-- Parameters:
--  {{blockchain}}: the chain for which we want to retrieve batch data

with
past_data_ethereum as (
    select
        s.environment,
        -1 as auction_id,
        d.block_number,
        from_hex(d.order_uid) as order_uid,
        from_hex(d.solver) as solver,
        from_hex(d.data.quote_solver) as quote_solver, --noqa: RF01
        from_hex(d.tx_hash) as tx_hash,
        cast(d.data.surplus_fee as decimal(38, 0)) as surplus_fee, --noqa: RF01
        cast(d.data.amount as decimal(38, 0)) as amount, --noqa: RF01
        cast(d.data.protocol_fee as decimal(38, 0)) as protocol_fee, --noqa: RF01
        from_hex(d.data.protocol_fee_token) as protocol_fee_token, --noqa: RF01
        cast(d.data.protocol_fee_native_price as decimal(38, 0)) as protocol_fee_native_price, --noqa: RF01
        case
            when d.data.quote_sell_amount = 'None' then null --noqa: RF01
            else cast(d.data.quote_sell_amount as decimal(38, 0)) --noqa: RF01
        end as quote_sell_amount,
        case
            when d.data.quote_buy_amount = 'None' then null --noqa: RF01
            else cast(d.data.quote_buy_amount as decimal(38, 0)) --noqa: RF01
        end as quote_buy_amount,
        case
            when cast(d.data.quote_gas_cost as varchar) = 'NaN' then null --noqa: RF01
            else cast(d.data.quote_gas_cost as decimal(38, 0)) --noqa: RF01
        end as quote_gas_cost,
        case
            when cast(d.data.quote_sell_token_price as varchar) = 'NaN' then null --noqa: RF01
            else cast(d.data.quote_sell_token_price as decimal(38, 0)) --noqa: RF01
        end as quote_sell_token_price,
        cast(d.data.partner_fee as decimal(38, 0)) as partner_fee, --noqa: RF01
        from_hex(d.data.partner_fee_recipient) as partner_fee_recipient, --noqa: RF01
        d.data.protocol_fee_kind --noqa: RF01
    from cowswap.raw_order_rewards as d inner join cow_protocol_ethereum.solvers as s on from_hex(d.solver) = s.address where d.block_number < 20866925
),

past_data_gnosis as ( --noqa: ST03
-- data from Jan 23, 2024 till Sept 23, 2024, are present here
    select --noqa: ST06
        s.environment,
        -1 as auction_id,
        d.block_number,
        d.order_uid,
        d.solver,
        d.quote_solver,
        d.tx_hash,
        cast(d.surplus_fee as decimal(38, 0)) as surplus_fee,
        cast(d.amount as decimal(38, 0)) as amount,
        cast(d.protocol_fee as decimal(38, 0)) as protocol_fee,
        d.protocol_fee_token,
        cast(d.protocol_fee_native_price as decimal(38, 0)) as protocol_fee_native_price,
        cast(d.quote_sell_amount as decimal(38, 0)) as quote_sell_amount,
        cast(d.quote_buy_amount as decimal(38, 0)) as quote_buy_amount,
        case
            when cast(d.quote_gas_cost as varchar) = 'NaN' then null
            else cast(d.quote_gas_cost as decimal(38, 0))
        end as quote_gas_cost,
        case
            when cast(d.quote_sell_token_price as varchar) = 'NaN' then null
            else cast(d.quote_sell_token_price as decimal(38, 0))
        end as quote_sell_token_price,
        cast(d.partner_fee as decimal(38, 0)) as partner_fee,
        d.partner_fee_recipient,
        d.protocol_fee_kind
    from dune.cowprotocol.dataset_cowswap_gnosis_raw_order_rewards as d inner join cow_protocol_gnosis.solvers as s on cast(d.solver as varchar) = cast(s.address as varchar)
),

past_data_arbitrum as ( --noqa: ST03
-- data till Sept 23, 2024, are present here
    select --noqa: ST06
        s.environment,
        -1 as auction_id,
        d.block_number,
        d.order_uid,
        d.solver,
        d.quote_solver,
        d.tx_hash,
        cast(d.surplus_fee as decimal(38, 0)) as surplus_fee,
        cast(d.amount as decimal(38, 0)) as amount,
        cast(d.protocol_fee as decimal(38, 0)) as protocol_fee,
        d.protocol_fee_token,
        case
            when d.protocol_fee_native_price = 'inf' then null
            else cast(d.protocol_fee_native_price as decimal(38, 0))
        end as protocol_fee_native_price,
        cast(d.quote_sell_amount as decimal(38, 0)) as quote_sell_amount,
        cast(d.quote_buy_amount as decimal(38, 0)) as quote_buy_amount,
        case
            when cast(d.quote_gas_cost as varchar) = 'NaN' then null
            else cast(d.quote_gas_cost as decimal(38, 0))
        end as quote_gas_cost,
        case
            when cast(d.quote_sell_token_price as varchar) = 'NaN' then null
            else cast(d.quote_sell_token_price as decimal(38, 0))
        end as quote_sell_token_price,
        cast(d.partner_fee as decimal(38, 0)) as partner_fee,
        d.partner_fee_recipient,
        d.protocol_fee_kind
    from dune.cowprotocol.dataset_cowswap_arbitrum_raw_order_rewards as d inner join cow_protocol_arbitrum.solvers as s on cast(d.solver as varchar) = cast(s.address as varchar)
)

select
    environment,
    auction_id,
    block_number,
    order_uid,
    solver,
    quote_solver,
    tx_hash,
    cast(surplus_fee as decimal(38, 0)) as surplus_fee,
    cast(amount as decimal(38, 0)) as amount,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    protocol_fee_token,
    cast(protocol_fee_native_price as decimal(38, 0)) as protocol_fee_native_price,
    cast(quote_sell_amount as decimal(38, 0)) as quote_sell_amount,
    cast(quote_buy_amount as decimal(38, 0)) as quote_buy_amount,
    cast(quote_gas_cost as decimal(38, 0)) as quote_gas_cost,
    case
        when quote_sell_token_price = 'inf' then null
        else cast(quote_sell_token_price as decimal(38, 0))
    end as quote_sell_token_price,
    cast(partner_fee as decimal(38, 0)) as partner_fee,
    partner_fee_recipient,
    protocol_fee_kind
from dune.cowprotocol.dataset_order_data_{{blockchain}}_2024_10
union all
select
    environment,
    auction_id,
    block_number,
    order_uid,
    solver,
    quote_solver,
    tx_hash,
    cast(surplus_fee as decimal(38, 0)) as surplus_fee,
    cast(amount as decimal(38, 0)) as amount,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    protocol_fee_token,
    cast(protocol_fee_native_price as decimal(38, 0)) as protocol_fee_native_price,
    cast(quote_sell_amount as decimal(38, 0)) as quote_sell_amount,
    cast(quote_buy_amount as decimal(38, 0)) as quote_buy_amount,
    cast(quote_gas_cost as decimal(38, 0)) as quote_gas_cost,
    case
        when quote_sell_token_price = 'inf' then null
        else cast(quote_sell_token_price as decimal(38, 0))
    end as quote_sell_token_price,
    cast(partner_fee as decimal(38, 0)) as partner_fee,
    partner_fee_recipient,
    protocol_fee_kind
from dune.cowprotocol.dataset_order_data_{{blockchain}}_2024_11
union all
select
    environment,
    auction_id,
    block_number,
    order_uid,
    solver,
    quote_solver,
    tx_hash,
    cast(surplus_fee as decimal(38, 0)) as surplus_fee,
    cast(amount as decimal(38, 0)) as amount,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    protocol_fee_token,
    cast(protocol_fee_native_price as decimal(38, 0)) as protocol_fee_native_price,
    cast(quote_sell_amount as decimal(38, 0)) as quote_sell_amount,
    cast(quote_buy_amount as decimal(38, 0)) as quote_buy_amount,
    cast(quote_gas_cost as decimal(38, 0)) as quote_gas_cost,
    cast(quote_sell_token_price as decimal(38, 0)) as quote_sell_token_price,
    cast(partner_fee as decimal(38, 0)) as partner_fee,
    partner_fee_recipient,
    protocol_fee_kind
from dune.cowprotocol.dataset_order_data_{{blockchain}}_2024_12
union all
select *
from past_data_{{blockchain}}

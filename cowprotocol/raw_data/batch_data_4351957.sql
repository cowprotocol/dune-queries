-- This query provides data related to rewards/payouts on a per batch auction level
-- for all auctions that had at least one winner.
-- Parameters:
-- {{blockchain}}: the chain for which we want to retrieve batch data
with
past_batch_data_ethereum as (
    select
        s.environment,
        -1 as auction_id,
        d.block_number,
        d.block_deadline,
        d.tx_hash,
        d.solver,
        cast(d.data.execution_cost as decimal(38, 0)) as execution_cost, --noqa: RF01
        cast(d.data.surplus as decimal(38, 0)) as surplus, --noqa: RF01
        cast(d.data.protocol_fee as decimal(38, 0)) as protocol_fee, --noqa: RF01
        cast(d.data.fee as decimal(38, 0)) as network_fee, --noqa: RF01
        cast(d.data.uncapped_payment_eth as decimal(38, 0)) as uncapped_payment_native_token, --noqa: RF01
        cast(d.data.capped_payment as decimal(38, 0)) as capped_payment, --noqa: RF01
        cast(d.data.winning_score as decimal(38, 0)) as winning_score, --noqa: RF01
        cast(d.data.reference_score as decimal(38, 0)) as reference_score --noqa: RF01
    from cowswap.raw_batch_rewards as d inner join cow_protocol_ethereum.solvers as s on d.solver = cast(s.address as varchar) where d.block_deadline < 20866925
),

past_batch_data_gnosis as ( --noqa: ST03
    select
        'a' as environment,
        0 as auction_id,
        0 as block_number,
        0 as block_deadline,
        '0x' as tx_hash,
        '0x' as solver,
        0 as execution_cost,
        0 as surplus,
        0 as protocol_fee,
        0 as network_fee,
        0 as uncapped_payment_native_token,
        0 as capped_payment,
        0 as winning_score,
        0 as reference_score
    where false
),

past_batch_data_arbitrum as ( --noqa: ST03
    select
        'a' as environment,
        0 as auction_id,
        0 as block_number,
        0 as block_deadline,
        '0x' as tx_hash,
        '0x' as solver,
        0 as execution_cost,
        0 as surplus,
        0 as protocol_fee,
        0 as network_fee,
        0 as uncapped_payment_native_token,
        0 as capped_payment,
        0 as winning_score,
        0 as reference_score
    where false
)

select
    environment,
    auction_id,
    block_number,
    block_deadline,
    cast(tx_hash as varchar) as tx_hash,
    cast(solver as varchar) as solver,
    cast(execution_cost as decimal(38, 0)) as execution_cost,
    cast(surplus as decimal(38, 0)) as surplus,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    cast(network_fee as decimal(38, 0)) as network_fee,
    cast(uncapped_payment_eth as decimal(38, 0)) as uncapped_payment_native_token,
    cast(capped_payment as decimal(38, 0)) as capped_payment,
    cast(winning_score as decimal(38, 0)) as winning_score,
    cast(reference_score as decimal(38, 0)) as reference_score
from dune.cowprotocol.dataset_batch_data_{{blockchain}}_2024_10
union all
select
    environment,
    auction_id,
    block_number,
    block_deadline,
    cast(tx_hash as varchar) as tx_hash,
    cast(solver as varchar) as solver,
    cast(execution_cost as decimal(38, 0)) as execution_cost,
    cast(surplus as decimal(38, 0)) as surplus,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    cast(network_fee as decimal(38, 0)) as network_fee,
    cast(uncapped_payment_eth as decimal(38, 0)) as uncapped_payment_native_token,
    cast(capped_payment as decimal(38, 0)) as capped_payment,
    cast(winning_score as decimal(38, 0)) as winning_score,
    cast(reference_score as decimal(38, 0)) as reference_score
from dune.cowprotocol.dataset_batch_data_{{blockchain}}_2024_11
union all
select
    environment,
    auction_id,
    block_number,
    block_deadline,
    cast(tx_hash as varchar) as tx_hash,
    cast(solver as varchar) as solver,
    cast(execution_cost as decimal(38, 0)) as execution_cost,
    cast(surplus as decimal(38, 0)) as surplus,
    cast(protocol_fee as decimal(38, 0)) as protocol_fee,
    cast(network_fee as decimal(38, 0)) as network_fee,
    cast(uncapped_payment_eth as decimal(38, 0)) as uncapped_payment_native_token,
    cast(capped_payment as decimal(38, 0)) as capped_payment,
    cast(winning_score as decimal(38, 0)) as winning_score,
    cast(reference_score as decimal(38, 0)) as reference_score
from dune.cowprotocol.dataset_batch_data_{{blockchain}}_2024_12
union all
select *
from past_batch_data_{{blockchain}}

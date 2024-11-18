-- This query gets the main info for every batch settlement of CoW Swap
-- Parameters
-- {{blockchain}}: string the blockchain to query

with
num_tokens_traded as (
    select
        tx_hash,
        count(distinct token) as num_tokens
    from (
        select
            evt_tx_hash as tx_hash,
            "buyToken" as token
        from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
        union all
        select
            evt_tx_hash as tx_hash,
            "sellToken" as token
        from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
    ) as all_tokens
    group by tx_hash
)

select
    block_time,
    num_trades,
    num_tokens,
    dex_swaps,
    batch_value,
    gas_used / num_trades as gas_per_trade,
    case
        when '{{blockchain}}' = 'ethereum' then concat('<a href="https://etherscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name), '</a>')
        when '{{blockchain}}' = 'gnosis' then concat('<a href="https://gnosisscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name), '</a>')
        when '{{blockchain}}' = 'arbitrum' then concat('<a href="https://arbiscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name), '</a>')
    end as solver,
    case
        when '{{blockchain}}' = 'ethereum' then concat('<a href="https://etherscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar), '</a>')
        when '{{blockchain}}' = 'gnosis' then concat('<a href="https://gnosisscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar), '</a>')
        when '{{blockchain}}' = 'arbitrum' then concat('<a href="https://arbiscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar), '</a>')
    end as tx_hash,
    gas_price / pow(10, 9) as gas_price,
    gas_used,
    tx_cost_usd,
    fee_value,
    call_data_size,
    unwraps,
    token_approvals,
    case -- noqa: ST01
        when tx_cost_usd > 0 then fee_value / tx_cost_usd
        else null
    end as coverage
from cow_protocol_{{blockchain}}.batches as b
inner join num_tokens_traded as n on n.tx_hash = b.tx_hash
inner join cow_protocol_{{blockchain}}.solvers
    on solver_address = address
where block_time > now() - interval '3' month
order by block_time desc;

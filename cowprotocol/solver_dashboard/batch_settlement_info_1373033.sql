-- Parameters
-- {{blockchain}} - blockchain name

with 
num_tokens_traded as (
    select 
        tx_hash,
        count(distinct(token)) num_tokens 
    from (
        select evt_tx_hash as tx_hash, "buyToken" token from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
        union
        select evt_tx_hash as tx_hash, "sellToken" token from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
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
    case when '{{blockchain}}'='ethereum' then
        CONCAT('<a href="https://etherscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name),  '</a>') 
        when '{{blockchain}}'='gnosis' then
        CONCAT('<a href="https://gnosisscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name),  '</a>') 
        when '{{blockchain}}'='arbitrum' then
        CONCAT('<a href="https://arbiscan.io/address/', cast(solver_address as varchar), '" target="_blank">', concat(environment, '-', name),  '</a>') 
    end as solver,
    
    case when '{{blockchain}}'='ethereum' then
        CONCAT('<a href="https://etherscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar),  '</a>') 
        when '{{blockchain}}'='gnosis' then
        CONCAT('<a href="https://gnosisscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar),  '</a>') 
        when '{{blockchain}}'='arbitrum' then
        CONCAT('<a href="https://arbiscan.io/tx/', cast(b.tx_hash as varchar), '" target="_blank">', cast(b.tx_hash as varchar),  '</a>') 
    end as tx_hash,
    gas_price / pow(10, 9) as gas_price,
    gas_used,
    tx_cost_usd,
    call_data_size,
    unwraps,
    token_approvals
from cow_protocol_{{blockchain}}.batches b
join num_tokens_traded n
    on n.tx_hash = b.tx_hash
join cow_protocol_{{blockchain}}.solvers
    on solver_address = address
where block_time > now() - interval '3' month
order by block_time desc;

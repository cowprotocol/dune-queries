WITH 
num_tokens_traded as (
    select 
        hash,
        count(distinct(token)) num_tokens 
    from (
        select evt_tx_hash as hash, "buyToken" token from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
        UNION
        select evt_tx_hash as hash, "sellToken" token from gnosis_protocol_v2_{{blockchain}}.GPv2Settlement_evt_Trade
    ) as all_tokens
    group by hash
)

SELECT 
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
        CONCAT('<a href="https://etherscan.io/tx/', cast(tx_hash as varchar), '" target="_blank">', cast(tx_hash as varchar),  '</a>') 
        when '{{blockchain}}'='gnosis' then
        CONCAT('<a href="https://gnosisscan.io/tx/', cast(tx_hash as varchar), '" target="_blank">', cast(tx_hash as varchar),  '</a>') 
        when '{{blockchain}}'='arbitrum' then
        CONCAT('<a href="https://arbiscan.io/tx/', cast(tx_hash as varchar), '" target="_blank">', cast(tx_hash as varchar),  '</a>') 
    end as txHash,
    gas_price / pow(10, 9) as gas_price,
    gas_used,
    tx_cost_usd,
    fee_value,
    call_data_size,
    unwraps,
    token_approvals,
    case when tx_cost_usd > 0 then fee_value / tx_cost_usd else null end as coverage 
FROM cow_protocol_{{blockchain}}.batches b
JOIN num_tokens_traded
    ON hash = tx_hash
JOIN cow_protocol_{{blockchain}}.solvers
    on solver_address = address
where block_time > now() - interval '3' month
ORDER BY block_time DESC;

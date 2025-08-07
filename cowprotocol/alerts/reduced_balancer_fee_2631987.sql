with 
swap_fee_changes as (
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'stable_phantom' as pool_type 
    from balancer_v2_ethereum.composablestablepool_evt_swapfeepercentagechanged
    union all
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'stable_phantom' as pool_type 
    from balancer_v2_ethereum.stablephantompool_evt_swapfeepercentagechanged
    union all
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'weighted' as pool_type 
    from balancer_v2_ethereum.weightedpool_evt_swapfeepercentagechanged
    union all
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'weighted_v2' as pool_type 
    from balancer_v2_ethereum.weightedpoolv2_evt_swapfeepercentagechanged
    union all
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'stable' as pool_type 
    from balancer_v2_ethereum.stablepool_evt_swapfeepercentagechanged
    union all
    select 
        evt_block_time as block_time, 
        evt_index,
        evt_tx_hash as tx_hash, 
        contract_address as pool, 
        SwapFeePercentage as swap_fee, --noqa: CP02
        'stable' as pool_type 
    from balancer_v2_ethereum.metastablepool_evt_swapfeepercentagechanged
)

select
    fc.*,
    solver_address
from swap_fee_changes as fc
inner join cow_protocol_ethereum.batches as b on fc.tx_hash = b.tx_hash
where
    b.block_time between cast('{{start_time}}' as timestamp) and cast('{{end_time}}' as timestamp)
    and fc.tx_hash not in (
        select tx_hash
        from swap_fee_changes
        group by tx_hash
        having count(*) > 1
    )
    and not exists (
        select 1
        from swap_fee_changes as fc2
        where
            fc.tx_hash = fc2.tx_hash
            and (
                fc.block_time > fc2.block_time
                or (fc.block_time = fc2.block_time and fc.evt_index > fc2.evt_index)
                or (fc.block_time = fc2.block_time and fc.evt_index > fc2.evt_index and fc.swap_fee > fc2.swap_fee)
            )
        order by fc2.block_time, fc2.evt_index desc
    )
order by fc.block_time, fc.evt_index desc

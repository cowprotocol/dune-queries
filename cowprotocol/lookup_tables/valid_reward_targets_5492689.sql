--noqa: disable=all
with full_bonding_pools as (
    select
        pool_address,
        pool_name,
        case
            when pool_name ='Gnosis DAO' and '{{blockchain}}' = 'lens' then 0x010af2e55f0539282c2601915c98a5cd276862aa
            when pool_name = 'CoW DAO' and '{{blockchain}}' = 'lens' then 0x798Bb2d0ac591E34a4068E447782De05c27eD160
            else creator
        end as creator
    from query_4056263
)

select distinct cowRewardTarget
from dune.cowprotocol.result_multichain_vouching_events as ve --https://dune.com/queries/5533098
inner join full_bonding_pools as vbp -- valid bonding pools
    on vbp.pool_address = ve.bondingPool
    and vbp.creator = ve.sender

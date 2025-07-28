--noqa: disable=all
select distinct cowRewardTarget
from dune.cowprotocol.result_multichain_vouching_events as ve --https://dune.com/queries/5533098
inner join query_4056263 as vbp -- valid bonding pools
    on vbp.pool_address = ve.bondingPool
    and vbp.creator = ve.sender

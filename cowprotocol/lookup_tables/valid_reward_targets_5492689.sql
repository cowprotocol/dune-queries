-- noqa: disable=all
select distinct cowRewardTarget--, 'ethereum' as blockchain
from cow_protocol_ethereum.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator

union distinct
select distinct cowRewardTarget--, 'gnosis' as blockchain
from cow_protocol_gnosis.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator

union distinct
select distinct cowRewardTarget--, 'base' as blockchain
from cow_protocol_base.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator

union distinct
select distinct cowRewardTarget--, 'arbitrum' as blockchain
from cow_protocol_arbitrum.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator

union distinct
select distinct cowRewardTarget--, 'avalanche_c' as blockchain
from cow_protocol_avalanche_c.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator

/*
union distinct
select distinct cowRewardTarget--, 'polygon' as blockchain
from cow_protocol_polygon.VouchRegister_evt_Vouch
inner join query_4056263 -- valid bonding pools
    on pool_address = bondingPool
    and sender = creator
*/

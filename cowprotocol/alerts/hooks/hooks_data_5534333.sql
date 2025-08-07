--      * this query returns
--               * hook success: where null means it was skipped, otherwise has the result of the execution
--               * hook data: all relevant data, such us the calldata, gas, target contract
--               * relevant order info: orderuid, time, tx_hash, and usd_value
--noqa: disable=all
with 
app_data_raw as (
    select *, 'prod' as environment from dune.cowprotocol.dataset_app_data_{{blockchain}}_prod
    union all
    select *, 'barn' as environment from dune.cowprotocol.dataset_app_data_{{blockchain}}_barn
)
,hooks_data as (
    select
        environment
        ,contract_app_data as app_hash
        ,json_extract_scalar(encode, '$.appcode') as app_code
        ,cast(json_extract_scalar(encode, '$.metadata.bridging.destinationchainid') as int) as destination_chain_id
        ,from_hex(substring(json_extract_scalar(encode, '$.metadata.bridging.destinationtokenaddress'), 3)) as destination_token_address
        ,json_extract(encode, '$.metadata.hooks.post') as post_hooks_data    
        ,json_extract(encode, '$.metadata.hooks.pre') as pre_hooks_data    
    from app_data_raw
)
,pre_hooks as (
    select
        environment
        ,app_hash
        ,app_code
        -- ,destination_chain_id
        -- ,destination_token_address
        ,'pre' as hook_type
        ,cast(json_extract(item, '$.dappid') as varchar) as app_id 
        ,cast(json_extract(item, '$.target') as varchar) as target
        ,cast(json_extract(item, '$.gaslimit') as double) as gas_limit 
        ,from_hex(substring(json_extract_scalar(item, '$.calldata'), 3)) as call_data
    from hooks_data,
        unnest(cast(pre_hooks_data as array<json>)) as t(item)
)
,post_hooks as (
    select
        environment
        ,app_hash
        ,app_code
        -- ,destination_chain_id
        -- ,destination_token_address
        ,'post' as hook_type
        ,json_extract_scalar(item, '$.dappid')  as app_id 
        ,json_extract_scalar(item, '$.target')  as target
        ,cast(json_extract_scalar(item, '$.gaslimit') as double) as gas_limit 
        ,from_hex(substring(json_extract_scalar(item, '$.calldata'), 3)) as call_data
    from hooks_data,
        unnest(cast(post_hooks_data as array<json>)) as t(item)
), 
hooks_union as (
    select * from post_hooks
    union all
    select * from pre_hooks
),
traces as (
    select success, block_time, tx_hash, input
    from {{blockchain}}.traces as t 
    where
        t.block_time >= date_add('{{lookback_time_unit}}', -{{lookback_units}}, now())
        and t."from" in (0x01dcb88678aedd0c4cc9552b20f4718550250574, 0x60bf78233f48ec42ee3f101b9a05ec7878728006) --hooks trampoline
)
select           
    '{{blockchain}}' as blockchain,
    t.block_time,
    hooks.hook_type,
    traces.success as hook_success, -- null (skipped hook), false (reverted hook), true (successful hook)    
    hooks.app_id as hook_app_id,
    hooks.target as hook_target,
    hooks.gas_limit as hook_gas_limit,
    t.usd_value,
    t.partial_fill,
    t.order_uid,
    t.tx_hash,
    t.app_data,
    hooks.call_data as hook_call_data
from cow_protocol_{{blockchain}}.trades as t
inner join hooks_union as hooks
    on hooks.app_hash = t.app_data
left outer join traces
    on hooks.call_data = traces.input
where 
    t.block_time >= date_add('{{lookback_time_unit}}', -{{lookback_units}}, now())
    and hooks.app_id is not null -- makes sure the order contains a hook

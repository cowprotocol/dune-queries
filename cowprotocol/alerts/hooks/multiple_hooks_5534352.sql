with 
all_hooks as (
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='ethereum')"
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='gnosis')"
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='base')"
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='arbitrum')"
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='avalanche_c')"
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='polygon')"    
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='lens')"    
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='bnb')"        
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='linea')"        
    union all
    select * 
    from "query_5534333(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='plasma')"    
)
select *
from (
    select
        *,
        count(1) over (partition by tx_hash, order_uid, hook_call_data, hook_target, hook_gas_limit) as hook_calls
    from all_hooks
    where 
        not(hook_app_id in (
            'cow-swap://libs/hook-dapp-lib/permit', 
            'PERMIT_TOKEN',
            'BUILD_CUSTOM_HOOK',
            '1db4bacb661a90fb6b475fd5b585acba9745bc373573c65ecc3e8f5bfd5dee1f',
            'cow.fi')
        )
        and not(hook_app_id like 'cow-sdk://flashloans/aave%')
)
where 
    hook_calls > 1
order by block_time desc, order_uid

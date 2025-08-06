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
)
select *
from (
    select 
        *,
        count(1) over (partition by hook_call_data) as hook_calls
    from all_hooks
)
where 
    hook_calls > 1 
    and hook_app_id not in ('PERMIT_TOKEN', '1db4bacb661a90fb6b475fd5b585acba9745bc373573c65ecc3e8f5bfd5dee1f')
order by block_time desc, order_uid

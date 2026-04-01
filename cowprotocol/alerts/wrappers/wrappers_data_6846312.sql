--noqa: disable=all
-- This query takes all wrapper calls coming from executed trades and looks at the blockchain traces to check if that call was successful 
with 
app_data_raw as (
    select *, 'prod' as environment from dune.cowprotocol.dataset_app_data_{{blockchain}}_prod
    union all
    select *, 'barn' as environment from dune.cowprotocol.dataset_app_data_{{blockchain}}_barn
)
, cleaned_app_data as (
    select * 
    from (
        select
            contract_app_data,
            try(json_parse(
                regexp_replace(
                    cast(encode as varchar),
                    '"quoteBody":"([^"\\\\]|\\\\.)*"',
                    '"quoteBody":"<REMOVED>"'
                )
            )) as encode
        from app_data_raw
    )
    where encode is not null
)
, wrappers_prep as (
    select
        contract_app_data as app_hash        
        ,cast(json_extract(encode, '$.environment') AS varchar) as environment
        ,json_extract_scalar(encode, '$.appCode') as app_code
        ,json_extract(encode, '$.metadata.wrappers') as wrappers
    from cleaned_app_data
    where json_extract(encode, '$.metadata.wrappers') is not null
)
, wrappers_data as (
    select
        environment
        ,app_hash
        ,app_code
        ,try(from_hex(json_extract_scalar(item, '$.address'))) as wrapper_address
        ,try(from_hex(json_extract_scalar(item, '$.data'))) as wrapper_data
        ,cast(json_extract_scalar(item, '$.isOmittable') as boolean) as is_omittable
    from wrappers_prep,
        unnest(cast(wrappers as array<json>)) as t(item)
)
, solver_traces as (
    select block_time, tx_hash, success, input, "from", "to"
    from {{blockchain}}.traces as t 
    where
        t.block_date >= date(date_add('{{lookback_time_unit}}', -{{lookback_units}}, now())) -- using date bc it's the partition field
        and type = 'call'
        and "from" in (select address from cow_protocol_{{blockchain}}.solvers)
        --and "to" = 0x891cf92cf082CD159aCAF6A62Ab010495B5Ab4aE
)
select           
    '{{blockchain}}' as blockchain,
    t.block_time,
    t.usd_value,
    tr.success as trace_success, 
    w.is_omittable,
    w.wrapper_address,
    w.wrapper_data,
    t.partial_fill,
    t.order_uid,
    t.tx_hash,
    w.app_hash 
from cow_protocol_{{blockchain}}.trades as t
inner join wrappers_data as w
    on w.app_hash = t.app_data
left join solver_traces as tr
    on t.tx_hash = tr.tx_hash
    and w.wrapper_address = tr."to"
    and bytearray_position(tr.input, w.wrapper_data) > 0 -- wrapper data is contained within the trace input
where   
    t.block_time >= date(date_add('{{lookback_time_unit}}', -{{lookback_units}}, now()))

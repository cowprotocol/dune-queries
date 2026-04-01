--noqa: disable=all
with 
multichain as (    
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='ethereum')"
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='gnosis')"
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='base')"
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='arbitrum')"
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='avalanche_c')"
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='polygon')"    
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='lens')"    
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='bnb')"        
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='linea')"        
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='plasma')"       
    union all
    select * from "query_6846312(lookback_time_unit='{{time_unit}}', lookback_units='{{units}}', blockchain='ink')"    
)
select *
from multichain
where
    is_omittable = false -- if true then solvers are allowed to skip 
    and trace_success is null 

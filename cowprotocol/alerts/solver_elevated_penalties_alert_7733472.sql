-- thresholds for firing an alert
with alert_thresholds as (
    select * from (
        values
        ('ethereum',    -0.2),
        ('gnosis',      -100.0),
        ('arbitrum',    -0.1),
        ('base',        -0.1),
        ('avalanche_c', -5.0),
        ('polygon',     -1000.0),
        ('bnb',         -0.1),
        ('linea',       -0.05),
        ('plasma',      -1000),
        ('ink',         -0.05)
    ) as t(blockchain, threshold)
),

-- this table aggregates rewards per production solver per chain
-- on the relevant time window by using the auction_data table
relevant_data as (
    select
        'ethereum' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='ethereum', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'gnosis' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='gnosis', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'arbitrum' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='arbitrum', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2  
    union all
    select
        'base' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='base', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'avalanche_c' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='avalanche_c', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'polygon' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='polygon', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'bnb' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='bnb', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'linea' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='linea', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'plasma' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='plasma', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
    union all
    select
        'ink' as blockchain,
        solver,
        sum(capped_payment)/1e18 as rewards_in_native_units
    from "query_5270914(blockchain='ink', start_time = '{{start_time}}', end_time = '{{end_time}}')"
    where environment = 'prod'
    group by 1,2
),

-- picking the relevant enties based on the alerts threshold
alerts as (
    select
        rd.blockchain,
        rd.solver,
        rd.rewards_in_native_units
    from relevant_data as rd inner join alert_thresholds as alt on rd.blockchain = alt.blockchain
    where rd.rewards_in_native_units <= alt.threshold
)

-- joining with solver names so that alerts are easier to read
select
    a.blockchain,
    a.solver as solver_address,
    s.name as solver_name,
    a.rewards_in_native_units
from alerts as a inner join dune.cowprotocol.solvers as s on a.blockchain = s.blockchain and a.solver = s.address
where s.environment = 'prod'

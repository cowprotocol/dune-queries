-- Alert query for monitoring solver inactivity per chain
-- Returns one row per chain where the solver has ZERO settlements in the lookback period
-- Returns nothing for chains with settlements (no alert for those chains)
-- Includes chains: gnosis, arbitrum, base, avalanche, polygon, bnb, linea, plasma
-- Note: Ethereum mainnet excluded as ExtQuasimodo does not operate there
--
-- Parameters:
--   {{solver_name}} - name of the solver to monitor (e.g., 'ExtQuasimodo')
--   {{environment}} - solver environment to check ('prod' or 'barn')
--   {{days_without_settlement}} - number of days to look back for settlements

with per_chain_stats as (
    -- Gnosis (xdai)
    select
        'gnosis' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_gnosis.solvers as s
    left join cow_protocol_gnosis.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Arbitrum
    select
        'arbitrum' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_arbitrum.solvers as s
    left join cow_protocol_arbitrum.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Base
    select
        'base' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_base.solvers as s
    left join cow_protocol_base.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Avalanche
    select
        'avalanche' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_avalanche_c.solvers as s
    left join cow_protocol_avalanche_c.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Polygon
    select
        'polygon' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_polygon.solvers as s
    left join cow_protocol_polygon.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- BNB Chain
    select
        'bnb' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_bnb.solvers as s
    left join cow_protocol_bnb.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Linea
    select
        'linea' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_linea.solvers as s
    left join cow_protocol_linea.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address

    union all

    -- Plasma
    select
        'plasma' as network,
        s.address as solver_address,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_plasma.solvers as s
    left join cow_protocol_plasma.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.environment = '{{environment}}' and s.active = true
    group by s.address
)

-- Returns one row per chain with ZERO settlements (triggers alert per chain)
-- Returns NOTHING for chains with settlements (no alert for active chains)
select
    '{{solver_name}}' as solver_name,
    '{{environment}}' as environment,
    network,
    solver_address,
    {{days_without_settlement}} as lookback_days,
    settlements,
    last_settlement,
    now() as check_time,
    'No settlements on ' || network || ' (' || '{{environment}}' || ') in the last ' || cast({{days_without_settlement}} as varchar) || ' days' as alert_reason
from per_chain_stats
where settlements = 0

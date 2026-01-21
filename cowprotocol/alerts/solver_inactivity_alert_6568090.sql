-- Alert query for monitoring solver inactivity per chain
-- Returns one row per chain where the solver has ZERO settlements in the lookback period
-- Returns nothing for chains with settlements (no alert for those chains)
-- Includes all chains: ethereum, gnosis, arbitrum, base, avalanche, polygon, bnb, linea, plasma
--
-- Parameters:
--   {{solver_name}} - name of the solver to monitor (e.g., 'ExtQuasimodo')
--   {{days_without_settlement}} - number of days to look back for settlements

with per_chain_stats as (
    -- Ethereum (mainnet)
    select
        'ethereum' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_ethereum.solvers as s
    left join cow_protocol_ethereum.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Gnosis (xdai)
    select
        'gnosis' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_gnosis.solvers as s
    left join cow_protocol_gnosis.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Arbitrum
    select
        'arbitrum' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_arbitrum.solvers as s
    left join cow_protocol_arbitrum.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Base
    select
        'base' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_base.solvers as s
    left join cow_protocol_base.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Avalanche
    select
        'avalanche' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_avalanche_c.solvers as s
    left join cow_protocol_avalanche_c.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Polygon
    select
        'polygon' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_polygon.solvers as s
    left join cow_protocol_polygon.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- BNB Chain
    select
        'bnb' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_bnb.solvers as s
    left join cow_protocol_bnb.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Linea
    select
        'linea' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_linea.solvers as s
    left join cow_protocol_linea.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true

    union all

    -- Plasma
    select
        'plasma' as network,
        count(b.tx_hash) as settlements,
        max(b.block_time) as last_settlement
    from cow_protocol_plasma.solvers as s
    left join cow_protocol_plasma.batches as b
        on b.solver_address = s.address and b.block_time > now() - interval '{{days_without_settlement}}' day
    where s.name = '{{solver_name}}' and s.active = true
)

-- Returns one row per chain with ZERO settlements (triggers alert per chain)
-- Returns NOTHING for chains with settlements (no alert for active chains)
select
    '{{solver_name}}' as solver_name,
    network,
    {{days_without_settlement}} as lookback_days,
    settlements,
    last_settlement,
    now() as check_time,
    'No settlements on ' || network || ' in the last ' || cast({{days_without_settlement}} as varchar) || ' days' as alert_reason
from per_chain_stats
where settlements = 0

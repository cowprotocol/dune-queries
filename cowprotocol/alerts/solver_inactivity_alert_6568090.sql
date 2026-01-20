-- Alert query for monitoring solver inactivity across all chains
-- Returns a row ONLY if the solver has ZERO settlements on ANY chain in the lookback period
-- Returns nothing if there are settlements (no alert)
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
),

aggregated as (
    select
        sum(settlements) as total_settlements,
        max(last_settlement) as most_recent_settlement,
        count(distinct case when settlements > 0 then network end) as active_chains,
        count(distinct network) as total_chains
    from per_chain_stats
)

-- Returns ONLY if there are ZERO settlements across ALL chains (triggers alert)
-- Returns NOTHING if there are settlements (no alert)
select
    '{{solver_name}}' as solver_name,
    {{days_without_settlement}} as lookback_days,
    total_settlements,
    most_recent_settlement,
    total_chains,
    active_chains,
    now() as check_time,
    'No settlements found on any chain in the last ' || cast({{days_without_settlement}} as varchar) || ' days' as alert_reason
from aggregated
where total_settlements = 0

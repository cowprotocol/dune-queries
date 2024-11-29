-- This query gets all the rewards awarded to each solver, up untill the cip 20 rules
-- It is decomposed by the different environments


with
--- The initial launch of solver rewards was March 2022.
--- Solvers were awarded 100 COW tokens per settled batch.
pre_cip_10 as (
    select
        solver_address as solver,
        cast(100 * count(*) as uint256) as cow_reward
    from cow_protocol_ethereum.batches
    -- CIP-10 Cutoff from Snapshot:
    -- https://snapshot.org/#/cow.eth/proposal/0x5ccfa8fb4ae80d62b35ca83591e9986aae85a3169c10b55d8dd53b33a191fd6b
    where
        block_date < date(timestamp '2022-06-21')
        and block_date > date(timestamp '2022-03-01') -- Start Date of Solver rewards
    group by solver_address
),

--- CIP-10 introduced a modification to the reward scheme from 100 COW per batch to:
--- 50 COW per batch + 35 COW per trade.
pre_cip14 as (
    select
        solver_address as solver,
        cast(50 * count(*) + 35 * sum(num_trades) as uint256) as cow_reward
    from cow_protocol_ethereum.batches
    -- pre CIP-14 https://snapshot.org/#/cow.eth/proposal/0x3c84fc8e3cfe9cc6df76198d5031fae6580d8f9531f5b92ca3adedbc976cb1e5
    where
        block_date < date(timestamp '2022-10-25')
        and block_date >= date(timestamp '2022-06-21') -- CIP-10 cutoff
    group by solver_address
),

--- Risky Batches are defined as 
--- batches containing interactions carrying revert risk due to changing prices (i.e. DEX interactions).
--- These are "weakly" classified by interactions which are NOT token approvals, transfers and (un)wrapping (W)ETH
--- This table gathers all transactions containing at least one interaction carrying revert risk.
risky_batches as (
    select distinct
        evt_tx_hash,
        false as risk_free
    from gnosis_protocol_v2_ethereum.GPv2Settlement_evt_Interaction
    where
        evt_block_time >= timestamp '2022-10-25'
        and selector not in (
            0x095ea7b3, -- approve
            0x2e1a7d4d, -- withdraw
            0xa9059cbb, -- transfer
            0x23b872dd  -- transferFrom
        )
),

--- CIP-14 introduced a "Risk Adjusted" reward scheme.
--- Rewards (per order) are computed in the backend with a revert risk factor as part of the forumlation.
--- These rewards are then stored in the backend and streamed into dune community sources.
post_cip14 as (
    select
        solver,
        --- Note that raw cow_reward is not necessarily the final reward valuation. 
        --- We also project the reward back down to 37 (the base reward for user orders) when the trade was contained in a risk-free batch.
        --- Furthermore, some order rewards are zero, these correspond to "Liquidity Orders" incoming from private market makers.
        --- Since these are considered a form of liquidity to fill user orders, they are not rewarded.
        sum(case when cow_reward > cast(0 as uint256) and s.tx_hash not in (select evt_tx_hash from risky_batches) then cast(37 as uint256) else cow_reward end) as cow_reward
    from cow_protocol_ethereum.batches as s
    inner join cow_protocol_ethereum.order_rewards as r
        on
            s.tx_hash = r.tx_hash
            and block_date >= date(timestamp '2022-10-25')
            and block_date < date(timestamp '2023-03-16')
    group by solver
),

combined as (
    select * from pre_cip_10
    union all
    select * from pre_cip14
    union all
    select * from post_cip14
),

final_tally as (
    select
        name as solver_name,
        -- solver,
        sum(case when environment = 'prod' then cow_reward else cast(0 as uint256) end) as cow_reward_prod,
        sum(case when environment = 'barn' then cow_reward else cast(0 as uint256) end) as cow_reward_barn,
        sum(cow_reward) as total_cow_rewarded
    from combined
    inner join cow_protocol_ethereum.solvers
        on solver = address
    where environment in ('barn', 'prod')
    group by name
    order by total_cow_rewarded desc
)

--- Now to fetch the latest USD prices.
select
    *,
    total_cow_rewarded * (
        select price
        from prices.minute
        where
            contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
            and blockchain = 'ethereum'
        order by timestamp desc
        limit 1
    ) as cow_rewarded_usd
from final_tally

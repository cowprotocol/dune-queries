-- Get all users who are using COW Protocol on any chain.
-- Get aggregates (tx_count, usd_volume, user_count) of other [dexes/aggregators, chain] pairs that are used by our users.
-- Join competitor usage and cow protocol usage on the same chain (using user address).
-- Form a cohorts that will be compared using the aggregates (tx_count, usd_volume, user_count) .
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{end_time}} - the trade timestamp for which the analysis should end (inclusive)

with all_transactions as (
    select
        tx_from,
        tx_hash,
        blockchain,
        project,
        amount_usd,
        block_time,
        product_type
    from
        "query_4836358(start_time='{{start_time}}', end_time='{{end_time}}')"
),

cow_protocol_target_users as (
    select
        tx_from as address,
        blockchain as chain_used_for_cow,
        count(*) as total_transactions_on_cow,
        sum(amount_usd) as total_volume_usd_on_cow
    from
        all_transactions
    where
        project = 'cow_protocol'
    group by
        1, 2
),

users_per_chain_cow as (
    select
        chain_used_for_cow,
        count(distinct address) as distinct_users_cow
    from
        cow_protocol_target_users
    group by
        1
),

chains_supported_by_cow as (
    select distinct chain_used_for_cow as blockchain
    from
        cow_protocol_target_users
),

all_competitor_transactions as (
    select
        tx_from as address,
        blockchain as chain_used_for_competitor,
        project as competitor_project,
        sum(amount_usd) as competitor_total_volume_usd,
        count(*) as competitor_total_transactions
    from
        all_transactions
    where
        project != 'cow_protocol'
        and
        blockchain in (select blockchain from chains_supported_by_cow)
    group by 1, 2, 3
),

agg as (
    select
        chain_used_for_cow,
        chain_used_for_competitor,
        competitor_project,
        count(distinct address) as distinct_users_competitor,
        sum(total_transactions_on_cow) as transactions_made_on_cow,
        sum(total_volume_usd_on_cow) as total_volume_usd_on_cow,
        sum(competitor_total_transactions) as transactions_made_on_competitor,
        sum(competitor_total_volume_usd) as total_volume_usd_on_competitor
    from
        cow_protocol_target_users
    left join all_competitor_transactions using (address) -- noqa: disable=L032
    group by 1, 2, 3
)

select
    chain_used_for_cow,
    chain_used_for_competitor,
    competitor_project,
    distinct_users_cow,
    distinct_users_competitor,
    transactions_made_on_cow,
    total_volume_usd_on_cow,
    transactions_made_on_competitor,
    total_volume_usd_on_competitor
from
    agg
left join users_per_chain_cow using (chain_used_for_cow) -- noqa: disable=L032

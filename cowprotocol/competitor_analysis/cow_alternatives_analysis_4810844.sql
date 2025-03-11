-- Get all users who are using COW Protocol on any chain but {{chain_of_interest}}
-- Get aggregates (tx_count, usd_volume, user_count) of other dexes/aggregators they use on all chains supported by cow
-- or limit the results to only competitor usage on {{chain_of_interest}} if {{show_competitors_on_target_chain_only}} is set to 1
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{end_time}} - the trade timestamp for which the analysis should end (inclusive)
--  {{chain_of_interest}} - which chain is not used by cohort of users we want to investigate
--  {{show_competitors_on_target_chain_only}} - filters competitor's usage to only {{chain_of_interest}}

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

all_cow_users as (
    select
        tx_from as address,
        array_agg(distinct blockchain) as uses_on_chains
    from
        all_transactions
    where
        project = 'cow_protocol'
    group by
        1
),

cow_users_filtered as (
    select *
    from
        all_cow_users
    where
        not contains(uses_on_chains, '{{chain_of_interest}}')
),

chains_supported_by_cow as (
    select distinct blockchain
    from
        all_transactions
    where
        project = 'cow_protocol'
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

joined_and_filtered as (
    select
        *,
        count(distinct address) over (partition by chain_used_for_competitor) as distinct_users_count_per_chain
    from
        cow_users_filtered
    inner join all_competitor_transactions using (address) -- noqa: disable=L032
    where
        {{show_competitors_on_target_chain_only}} = 0 or chain_used_for_competitor = '{{chain_of_interest}}'
),

aggregated as (
    select
        chain_used_for_competitor,
        competitor_project,
        sum(competitor_total_volume_usd) as competitor_total_volume_usd,
        sum(competitor_total_transactions) as competitor_total_transactions,
        count(distinct address) as distinct_users_count_per_product_per_chain,
        any_value(distinct_users_count_per_chain) as distinct_users_count_per_chain
    from
        joined_and_filtered
    group by 1, 2
)


select
    chain_used_for_competitor,
    competitor_project,
    competitor_total_volume_usd,
    competitor_total_transactions,
    distinct_users_count_per_product_per_chain,
    distinct_users_count_per_chain
from
    aggregated
order by 3 desc

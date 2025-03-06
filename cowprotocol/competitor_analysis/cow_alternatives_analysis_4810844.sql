-- Get all users who are using COW Protocol on any chain but {{chain_of_interest}}
-- Get aggregates (tx_count, usd_volume, user_count) of other dexes/aggregators they use on all chains supported by cow
-- or limit the results to only competitor usage on {{chain_of_interest}} if {{show_competitors_on_target_chain_only}} is set to 1
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{end_time}} - the trade timestamp for which the analysis should end (inclusive)
--  {{chain_of_interest}} - which chain is not used by cohort of users we want to investigate
--  {{show_competitors_on_target_chain_only}} - filters competitor's usage to only {{chain_of_interest}}

with all_cow_users as (
    select
        tx_from as address,
        array_agg(distinct blockchain) as uses_on_chains
    from
        dex_aggregator.trades
    where
        project = 'cow_protocol'
        and
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
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
        dex_aggregator.trades
    where
        project = 'cow_protocol'
),

trades_dex_and_aggregators as (
    select
        tx_from,
        blockchain,
        project,
        amount_usd
    from
        dex.trades
    where
        blockchain in (select blockchain from chains_supported_by_cow)
        and
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'

    union all

    select
        tx_from,
        blockchain,
        project,
        amount_usd
    from
        dex_aggregator.trades
    where
        project != 'cow_protocol'
        and
        blockchain in (select blockchain from chains_supported_by_cow)
        and
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
),

all_competitor_transactions as (
    select
        tx_from as address,
        blockchain as chain_used_for_competitor,
        project as competitor_project,
        sum(amount_usd) as competitor_total_volume_usd,
        count(*) as competitor_total_transactions
    from
        trades_dex_and_aggregators
    group by 1, 2, 3
),

joined_and_filtered as (
    select *
    from
        cow_users_filtered
    inner join all_competitor_transactions using (address) -- noqa: disable=L032
    where
        {{show_competitors_on_target_chain_only}} = 0 or chain_used_for_competitor = '{{chain_of_interest}}'
),

user_count_per_chain as (
    select
        chain_used_for_competitor,
        count(distinct address) as distinct_users_count_per_chain
    from
        joined_and_filtered
    group by
        1
),

aggregated as (
    select
        chain_used_for_competitor,
        competitor_project,
        sum(competitor_total_volume_usd) as competitor_total_volume_usd,
        sum(competitor_total_transactions) as competitor_total_transactions,
        count(distinct address) as distinct_users_count
    from
        joined_and_filtered
    group by 1, 2
)


select
    chain_used_for_competitor,
    competitor_project,
    competitor_total_volume_usd,
    competitor_total_transactions,
    distinct_users_count as distinct_users_count_per_product_per_chain,
    distinct_users_count_per_chain
from
    aggregated
left join user_count_per_chain using (chain_used_for_competitor) -- noqa: disable=L032
order by 3 desc

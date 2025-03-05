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

all_competitor_transactions as (
    select
        tx_from as address,
        blockchain as chain_used_for_competitor,
        project as competitor_project,
        sum(amount_usd) as competitor_total_volume_usd,
        count(*) as competitor_total_transactions
    from
        dex.trades
    where
        blockchain in (select blockchain from chains_supported_by_cow)
        and
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
    group by 1, 2, 3

    union all

    select
        tx_from as address,
        blockchain as chain_used_for_competitor,
        project as competitor_project,
        sum(amount_usd) as competitor_total_volume_usd,
        count(*) as competitor_total_transactions
    from
        dex_aggregator.trades
    where
        project != 'cow_protocol'
        and
        blockchain in (select blockchain from chains_supported_by_cow)
        and
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
    group by 1, 2, 3
)

select
    chain_used_for_competitor,
    competitor_project,
    sum(competitor_total_volume_usd) as competitor_total_volume_usd,
    sum(competitor_total_transactions) as competitor_total_transactions,
    count(distinct address) as distinct_users_count
from
    cow_users_filtered
left join all_competitor_transactions using (address) -- noqa: disable=L032
where
    {{show_competitors_on_target_chain_only}} = 0 or chain_used_for_competitor = '{{chain_of_interest}}'
group by 1, 2
order by 3 desc

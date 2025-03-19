-- Calculates month over month retention per project per date. Use {{lookback_window_months}} to see different retention periods.
-- Warning - this is not a cohort retention!!!
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{lookback_window_months}} - month of retention we would like to do the analysis for
--  {{chain_name}} - chain to do the analysis for

with all_users as (
    select distinct
        curr_aggregate_by.taker as user_address,
        date_trunc('Month', curr_aggregate_by.block_date) as aggregate_by,
        case when project in ('1inch', '1inch Limit Order Protocol') then '1inch' else project end as project
    from
        dex_aggregator.trades as curr_aggregate_by
    where
        blockchain = '{{chain_name}}'
        and
        block_date >= timestamp '{{start_time}}' - interval '{{lookback_window_months}}' month
)

select
    active.aggregate_by,
    active.project,
    count(distinct retained.user_address) as retained_users,
    count(distinct active.user_address) as active_users,
    cast(count(distinct retained.user_address) as double) / cast(count(distinct active.user_address) as double) as retention_rate
from
    all_users as active
left join
    all_users as retained
    on
        active.user_address = retained.user_address
        and
        active.project = retained.project
        and
        active.aggregate_by = retained.aggregate_by - interval '{{lookback_window_months}}' month
group by
    1, 2
order by
    1, 2

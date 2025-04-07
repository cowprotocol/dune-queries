-- Query that calculates for each [user, cal. month] pair a user status that is one of ['Loyal', 'Opportunistic', 'Lost', 'Gone', 'Didnt exist']
-- Then it aggregates it by month and calculates transitions from one status to another month over month.
-- It is parametrised by chain and project.
-- Status descriptions:
-- Loyal - only traded {{project}} during calendar month
-- Opportunistic - traded both {{project}} during calendar month and other projects
-- Gone - was created before date of interest but didn't trade in given calendar month
-- Didnt exist - user was created this month (this status is only possible for prev_status column)
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{chain_name}} - chain to do the analysis for
--  {{project}} - project to do the analysis for
--  {{current_status}} - filter to limit analysis to users that had this status as current for given calendar month
--  {{previous_status}} - filter to limit analysis to users that had this status as previous for given calendar month


with creation_month_per_user as (
    -- TODO: use both dex and dex-agg here
    select
        taker as user_address,
        min(date_trunc('Month', block_date)) as creation_month
    from
        dex_aggregator.trades
    where
        blockchain = '{{chain_name}}'
    group by 1
),

all_active_users as (
    -- TODO: use layered txs here to prevent miss-information
    select distinct
        taker as user_address,
        date_trunc('Month', block_date) as block_month,
        case when project in ('1inch', '1inch Limit Order Protocol') then '1inch' else project end as project
    from
        dex_aggregator.trades
    where
        blockchain = '{{chain_name}}'
        and
        block_date >= timestamp '{{start_time}}' - interval '1' month
),

users_backbone as (
    select
        user_address,
        a.dt as block_month,
        null as project
    from
        all_active_users
    cross join unnest(sequence(date_trunc('Month', timestamp '{{start_time}}') - interval '1' month, current_date, interval '1' month)) as a (dt)
),

all_users as (
    select *
    from
        users_backbone

    union all

    select *
    from
        all_active_users
),

user_w_creation_date as (
    select
        all_users.user_address,
        block_month,
        creation_month,
        project
    from
        all_users
    left join creation_month_per_user on all_users.user_address = creation_month_per_user.user_address
    where
        creation_month <= block_month
),

users_labeled as (
    select
        user_address,
        block_month,
        any_value(creation_month) as creation_month,
        case
            when count(distinct project) = 1 and max(project) = '{{project}}' then 'Loyal'
            when contains(array_agg(project) filter (where project is not null), '{{project}}') then 'Opportunistic'
            when count(distinct project) >= 1 and not contains(array_agg(project) filter (where project is not null), '{{project}}') then 'Lost'
            else 'Gone'
        end as user_type
    from
        user_w_creation_date
    group by 1, 2
),

users_w_last_month_status as (
    select
        user_address,
        block_month,
        creation_month,
        user_type as current_status,
        lag(user_type, 1, if(creation_month >= block_month, 'Didnt Exist', 'Gone')) over (partition by user_address order by block_month asc) as previous_status
    from
        users_labeled
)


select
    block_month,
    previous_status || '->' || current_status as status_shift,
    count(distinct user_address) as users_count,
    '{{current_status}}' as debug_1,
    '{{previous_status}}' as debug_2
from
    users_w_last_month_status
where
    contains(split('{{current_status}}', ','), current_status)
    and
    contains(split('{{previous_status}}', ','), previous_status)
    and
    block_month >= timestamp '{{start_time}}'
group by 1, 2
order by 1, 3

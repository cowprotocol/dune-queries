with performance_data_per_auction as (
    select
        auction_time,
        accounting_week_start,
        auction_id,
        solver,
        reward as current_reward,
        volume,
        protocol_fee,
        case when uncapped_reward <= 0 then reward else least(uncapped_reward, {{fee_cap}} * protocol_fee) end as performance_reward,
        case when uncapped_reward <= 0 then {{fee_cap}} * protocol_fee else greatest({{fee_cap}} * protocol_fee - uncapped_reward, 0) end as consistency_budget
    from dune.cowprotocol.result_performance_data_per_auction_oct_2025_feb_2026
    where
        blockchain = '{{blockchain}}'
        and auction_time >= (timestamp '{{start_time}}') and auction_time < (timestamp '{{end_time}}')
),

performance_data_per_auction_filtered as (
    select * from performance_data_per_auction
    where solver != {{solver}} or current_reward < 0 or volume / 1e18 >= {{volume_filter}} -- this descreases performance rewards for {{solver}} but does not make other solvers win more rewards
),

performance_rewards as (
    select
        accounting_week_start,
        solver,
        sum(current_reward) as current_reward,
        sum(performance_reward) as performance_reward,
        sum(consistency_budget) as consistency_budget,
        sum(volume) as volume,
        sum(protocol_fee) as protocol_fee
    from performance_data_per_auction_filtered
    group by 1, 2
),

data_per_trade as (
    select
        block_time as auction_time,
        date_add('day', -((day_of_week(date_trunc('day', block_time)) + 5) % 7), date_trunc('day', block_time)) as accounting_week_start,
        rod.auction_id,
        rod.order_uid,
        t.order_type,
        t.limit_sell_amount,
        t.limit_buy_amount,
        rod.protocol_fee_native_price,
        case when t.order_type = 'SELL' then t.atoms_bought * rod.protocol_fee_native_price else t.atoms_sold * rod.protocol_fee_native_price end as volume
    from cow_protocol_{{blockchain}}.trades as t
    inner join "query_4364122(blockchain='{{blockchain}}')" as rod on t.order_uid = rod.order_uid and t.tx_hash = rod.tx_hash
    inner join performance_data_per_auction as pd on rod.auction_id = pd.auction_id
),

proposed_solutions as (
    select * from query_6741796
    where blockchain = '{{blockchain}}'
),

proposed_trade_executions as (
    select * from query_6741982
    where blockchain = '{{blockchain}}'
),

proposed_solutions_filtered as (
    select ps.*
    from proposed_solutions as ps
    where
        ps.solver != {{solver}}
        or (
            select sum(td.volume / 1e18) >= {{volume_filter}}
            from proposed_trade_executions as pte
            inner join data_per_trade as td on pte.order_uid = td.order_uid
            where pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
        )
),

metric_nr_orders as (
    select
        t.accounting_week_start,
        ps.solver,
        count(distinct t.order_uid) as metric
    from data_per_trade as t
    inner join proposed_trade_executions as pte on t.order_uid = pte.order_uid
    inner join proposed_solutions_filtered as ps on pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
    where ps.filtered_out = false
    group by 1, 2
),

surplus_per_order as (
    select
        t.accounting_week_start,
        ps.auction_id,
        ps.solver,
        t.order_uid,
        t.order_type,
        max(pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) as surplus,
        max((pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) * t.protocol_fee_native_price) as surplus_native,
        rank() over (
            partition by t.order_uid
            order by max(pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) desc
        ) as rk
    from data_per_trade as t
    inner join proposed_trade_executions as pte on t.order_uid = pte.order_uid
    inner join proposed_solutions_filtered as ps on pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
    where ps.filtered_out = false
    group by 1, 2, 3, 4, 5
),

metric_top4_orders as (
    select
        accounting_week_start,
        solver,
        sum(12.0 / solvers_on_order) as metric
    from (
        select
            accounting_week_start,
            solver,
            order_uid,
            count(*) over (partition by order_uid) as solvers_on_order
        from surplus_per_order
        where rk <= 4
    )
    group by 1, 2
),

participation_rates as (
    select
        accounting_week_start,
        solver,
        cast(count(distinct order_uid) as double) / max(total_orders) as participation_rate
    from (
        select
            accounting_week_start,
            solver,
            order_uid,
            count(distinct order_uid) over (partition by accounting_week_start) as total_orders
        from surplus_per_order
        where order_type = 'SELL'
    )
    group by 1, 2
),

robust_surplus_per_bid as (
    select
        spo.accounting_week_start,
        spo.solver,
        spo.order_uid,
        spo.surplus_native,
        p.participation_rate,
        spo.rk,
        coalesce(
            exp(sum(ln(1.0 - p.participation_rate)) over (
                partition by spo.order_uid
                order by spo.rk
                rows between unbounded preceding and 1 preceding
            )),
            1.0
        ) as remaining_prob
    from surplus_per_order as spo
    inner join participation_rates as p on spo.accounting_week_start = p.accounting_week_start and spo.solver = p.solver
    where spo.order_type = 'SELL'
),

marginal_contributions as (
    select
        accounting_week_start,
        solver,
        order_uid,
        remaining_prob * participation_rate * surplus_native as contribution,
        remaining_prob * participation_rate * surplus_native
        - participation_rate / (1.0 - participation_rate)
        * coalesce(sum(remaining_prob * participation_rate * surplus_native) over (
            partition by order_uid
            order by rk
            rows between 1 following and unbounded following
        ), 0) as marginal_contribution
    from robust_surplus_per_bid
),

metric_robust_surplus as (
    select
        accounting_week_start,
        solver,
        sum(marginal_contribution) as metric
    from marginal_contributions
    group by 1, 2
),

consistency_budget as (
    select
        accounting_week_start,
        sum(consistency_budget) as consistency_budget
    from performance_rewards
    group by 1
),

consistency_metric_normalization as (
    select
        accounting_week_start,
        sum(metric) as metric
    from {{consistency_metric}}
    group by 1
),

consistency_reward_per_unit as (
    select
        cb.accounting_week_start,
        consistency_budget / metric as consistency_reward_per_unit
    from consistency_budget as cb
    join consistency_metric_normalization as cmn on cb.accounting_week_start = cmn.accounting_week_start
),

consistency_rewards as (
    select
        cm.accounting_week_start,
        solver,
        consistency_reward_per_unit * metric as consistency_reward
    from {{consistency_metric}} as cm
    join consistency_reward_per_unit as crpu on cm.accounting_week_start = crpu.accounting_week_start
)
-- select * from consistency_rewards

select
    -- pr.accounting_week_start,
    pr.solver,
    sum(current_reward / 1e18) as current_reward,
    sum((performance_reward + consistency_reward) / 1e18) as new_reward,
    sum(performance_reward / 1e18) as performance_reward,
    sum(consistency_reward / 1e18) as consistency_reward,
    sum(protocol_fee / 1e18) as protocol_fee,
    sum(volume / 1e18) as volume
from performance_rewards as pr
join consistency_rewards as cr on pr.accounting_week_start = cr.accounting_week_start and pr.solver = cr.solver
group by 1
-- order by 1 desc, 3 desc
order by 2 desc

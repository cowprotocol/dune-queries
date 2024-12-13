-- Computes volume, tvl and APR for Curve pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Parameters:
-- {{blockchain}}: The blockchain to query
-- {{start_time}}: The start time of the analysis. date '{{start_time}}' <= evt_block_time < date '{{start_time}}' + 1 day
--      By default, we look at the past full day

with accumulated_kpis as (
    select
        r.contract_address,
        fee,
        tvl,
        sum(amount_usd) over (partition by r.contract_address order by latest_per_pool desc) as volume,
        365 * sum(amount_usd * fee / (reserve0 * p0.price * power(10, -p0.decimals) + reserve1 * p1.price * power(10, -p1.decimals))) over (partition by r.contract_address order by latest_per_pool desc) as apr,
        -- new index to make sure rows don't get lost in the filtering later
        row_number() over (partition by r.contract_address order by r.evt_block_time desc) as latest_per_pool
    from "query_4232976(blockchain='{{blockchain}}', number_of_pools = '{{number_of_pools}}', start_time = '{{start_time}}')" as r
    left join
        ( --noqa: ST05
            select *
            from curve.trades
            where
                block_time >= least(date('{{start_time}}'), date_add('day', -1, date(now())))
        ) as t
        on
            r.contract_address = t.project_contract_address
            and r.tx_hash = t.tx_hash
    inner join prices.minute as p0
        on
            r.token0 = p0.contract_address
            and date_trunc('minute', r.evt_block_time) = p0.timestamp
    inner join prices.minute as p1
        on
            r.token1 = p1.contract_address
            and date_trunc('minute', r.evt_block_time) = p1.timestamp
    where
        -- This test avoids any possible issue with reconstructing the reserves of the pool
        tvl > 0
        and p0.timestamp between least(date('{{start_time}}'), date_add('day', -1, date(now()))) and least(date_add('day', 1, date('{{start_time}}')), date(now()))
        and p1.timestamp between least(date('{{start_time}}'), date_add('day', -1, date(now()))) and least(date_add('day', 1, date('{{start_time}}')), date(now()))
)

select
    contract_address,
    fee,
    tvl,
    volume,
    apr
from accumulated_kpis
where latest_per_pool = 1

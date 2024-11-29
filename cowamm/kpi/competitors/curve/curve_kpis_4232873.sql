-- Computes volume, tvl and APR for Curve pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Parameters:
-- {{blockchain}}: The blockchain to query
-- {{competitor_end_time}}: The end time of the time window (end_time - 1 day; end_time), defaults to now()
select
    contract_address,
    tvl,
    fee,
    volume,
    apr
from (
    select
        contract_address,
        fee,
        tvl,
        latest_per_pool,
        sum(amount_usd) over (partition by contract_address order by latest_per_pool) as volume,
        365 * sum(amount_usd * fee / tvl) over (partition by contract_address order by latest_per_pool) as apr
    -- The first call to 4232976 gets the tvl after each tx to compute volume/tvl
    from "query_4232976(blockchain='{{blockchain}}')" as r
    left join
        ( --noqa: ST05
            select *
            from curve.trades
            where
                block_time >= date_add('day', -1, (case when '{{competitor_end_time}}' = '2100-01-01' then now() else timestamp '{{competitor_end_time}}' end))
        ) as t
        on
            r.contract_address = t.project_contract_address
            and r.tx_hash = t.tx_hash
    where
        -- This test avoids any possible issue with reconstructing the reserves of the pool
        tvl > 0
)
where latest_per_pool = 1

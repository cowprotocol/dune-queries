-- Computes volume, tvl and APR for Curve pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Parameters:
-- {{blockchain}}: The blockchain to query
-- {{end_time}}: The end time of the time window (end_time - 1 day; end_time), defaults to now()
select
    r1.contract_address,
    r1.fee,
    r1.tvl,
    sum(amount_usd) as volume,
    365 * sum(amount_usd * r.fee / r.tvl) as apr
from "query_4232976(blockchain='{{blockchain}}')" as r
inner join "query_4232976(blockchain='{{blockchain}}')" as r1
    on
        r.contract_address = r1.contract_address
left join curve.trades as t
    on
        r.contract_address = t.project_contract_address
        and r.tx_hash = t.tx_hash
where
    t.block_time >= date_add('day', -1, (case when '{{end_time}}' = '2024-01-01' then now() else timestamp '{{end_time}}' end))
    and r1.latest = 1
    -- This test avoids any possible issue with reconstructing the reserves of the pool
    and r1.tvl > 0
group by 1, 2, 3

-- Computes volume, tvl and APR for Curve pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Parameters:
-- {{blockchain}}: The blockchain to query
select
    r.contract_address,
    fee,
    tvl,
    sum(amount_usd) as volume,
    365 * sum(amount_usd * fee / tvl) as apr
from "query_4232976(blockchain='{{blockchain}}')" as r
left join curve.trades as t
    on
        r.contract_address = t.project_contract_address
        and r.tx_hash = t.tx_hash
where
    t.block_time >= date_add('day', -1, now())
    -- This test avoids any possible issue with reconstructing the reserves of the pool
    and tvl > 0
group by r.contract_address
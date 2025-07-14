-- Computes volume, tvl and APR for CoW AMM pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Parameters:
-- {{blockchain}}: The blockchain to query
-- {{competitor_end_time}}: The end time of the time window (end_time - 1 day; end_time), defaults to now()

-- computes, surplus, tvl and thus relative surplus (per $100)
select
    contract_address,
    tvl,
    volume,
    apr
from ( --noqa: ST05
    select
        contract_address,
        tvl,
        latest_per_pool,
        sum(usd_value) over (partition by contract_address order by latest_per_pool desc) as volume,
        365 * sum(surplus_usd / tvl) over (partition by contract_address order by latest_per_pool desc) as apr
    from "query_4340356(blockchain='{{blockchain}}')" as tvl
    left join
        ( --noqa: ST05
            select *
            from cow_protocol_{{blockchain}}.trades
            where
                block_time >= date_add('day', -1, (case when '{{competitor_end_time}}' = '2100-01-01' then now() else timestamp '{{competitor_end_time}}' end))
        ) as t
        on
            tvl.tx_hash = t.tx_hash
            and tvl.contract_address = trader
)
where latest_per_pool = 1

-- Computes the trading volume of the buffers
-- The heuristic is:
-- For each transaction, we account for all the transfers through the settlement contract
-- If a token comes in but never comes out or reciprocally it is considered as volume

-- Parameters
--  {{blockchain}} - chain for which the query is running
--  {{start_time}}: rounded to a date
--  {{end_time}}: rounded to a date
--      the period is date start_time <= start_time <= date end_time

with transfers as (
    select
        contract_address,
        evt_block_time,
        -- net_inflow is the net value (positive if in, negative if out) for each token which comes in the GPV2Settlement contract
        sum(case when to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 then value else -value end) as net_inflow
    from erc20_{{blockchain}}.evt_transfer
    where
        evt_block_time between (date '{{start_time}}') and (date '{{end_time}}')
        and (
            to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
            or "from" = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
        )
    group by
        contract_address,
        evt_block_time
)

select sum(abs(net_inflow) * power(10, -decimals) * price) / 2 as trading_volume
from prices.minute as p
inner join transfers as t
    on
        p.contract_address = t.contract_address
        and timestamp = date_trunc('minute', evt_block_time)
--improves slightly efficiency as it reduces the number of rows
where
    not (net_inflow = 0)
    and timestamp between (date '{{start_time}}') and (date '{{end_time}}')
    and blockchain = '{{blockchain}}'

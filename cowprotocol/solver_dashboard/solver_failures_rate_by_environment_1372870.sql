--First get the nb of failures and successes for each solver
--Then get each solver's name and compute the success rate
--Finally, display the stats for prod and barn solvers
with 
settlement_transactions as (
    select
        date_trunc('{{Frequence}}', block_time) as time,
        "from" as solver,
        sum(case when success then 1 else 0 end) as successes,
        sum(case when not success then 1 else 0 end) as failures
    from {{blockchain}}.transactions
    where
        block_time > now() - interval '{{LastNDays}}' day
        and to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        and position('0x13d79a0b' in cast(data as varchar)) > 0 --! settle method ID
    group by "from", date_trunc('{{Frequence}}', block_time)
),

results as (
    select
        time,
        name as solver,
        environment,
        successes,
        failures,
        (case when successes = 0 then 0 else 1.00 * failures / (successes + failures) end) as failure_rate
    from settlement_transactions
    inner join cow_protocol_{{blockchain}}.solvers on solver = address
)

select * from results
where environment in ('prod', 'barn')
order by time desc

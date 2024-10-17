with 

latest_cow_price as (
    select price from prices.usd_latest 
    where blockchain = 'ethereum' 
    and contract_address = 0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB
),

solver_cow_rewards as (
    select 
        sum(value) / pow(10, 18) as cow_amount
    from cow_protocol_ethereum.CowProtocolToken_evt_Transfer
    where "from" = 0xa03be496e67ec29bc62f01a428683d7f9c204930 -- Rewards Payout Safe
),

grand_totals as (
    select 
        cow_amount as total_cow_rewarded,
        cow_amount * (select price from latest_cow_price limit 1) as total_value_rewarded,
        day(date(now()) - date(cast('2022-03-01' as timestamp))) as days_since_inception
    from solver_cow_rewards
),

final_results as (
select *,
    total_cow_rewarded / days_since_inception as average_daily_cow, 
    total_value_rewarded / days_since_inception as average_daily_value,
    365.0 * total_cow_rewarded / days_since_inception as projected_annual_cow, 
    365.0 * total_value_rewarded / days_since_inception as projected_annual_value
from grand_totals
),

vertical_results as (
    select 1 as rk, 'Total' as cow_distributed, total_cow_rewarded as cow, total_value_rewarded as usd from final_results
    union
    select 2 as rk, 'Average daily' as cow_distributed, average_daily_cow as cow, average_daily_value as usd from final_results
    union
    select 3 as rk, 'Projected Annual' as cow_distributed, projected_annual_cow as cow, projected_annual_value as usd from final_results
) 

select cow_distributed, cow, usd from vertical_results
order by rk

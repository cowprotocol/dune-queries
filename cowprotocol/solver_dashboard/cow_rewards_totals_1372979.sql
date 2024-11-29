-- This query computes the total amount of COW distributed as rewards
-- it uses all of the outgoing transactions from the rewards safe
-- and the price is converted to USD using the latest price from the prices.minute_latest table
-- the inception date is defined as 2022-03-01

-- finally the query calculates the daily payout and the project budget for the year
-- the first table addresses contains the parameters for each blockchain: COW token address and rewards safe address
-- To add a new blockchain, add a new row to the addresses table with the correct info

with
addresses as (
    select
        'ethereum' as blockchain,
        0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB as token,
        0xa03be496e67ec29bc62f01a428683d7f9c204930 as rewards_safe
    union distinct
    select
        'arbitrum' as blockchain,
        0xcb8b5CD20BdCaea9a010aC1F8d835824F5C87A04 as token,
        0x as rewards_safe
    union distinct
    select
        'gnosis' as blockchain,
        0x177127622c4A00F3d409B75571e12cB3c8973d3c as token,
        0x as rewards_safe
),

latest_cow_price as (
    select price from prices.minute_latest
    where
        blockchain = '{{blockchain}}'
        and contract_address = (select token from addresses where blockchain = '{{blockchain}}')
),

solver_cow_rewards as (
    select sum(value) / pow(10, 18) as cow_amount
    from erc20_ethereum.evt_transfer
    where
        "from" = (select rewards_safe from addresses where blockchain = '{{blockchain}}')
        and contract_address = (select token from addresses where blockchain = '{{blockchain}}')
),

grand_totals as (
    select
        cow_amount as total_cow_rewarded,
        cow_amount * (select price from latest_cow_price limit 1) as total_value_rewarded,
        day(date(now()) - date(cast('2022-03-01' as timestamp))) as days_since_inception
    from solver_cow_rewards
),

final_results as (
    select
        *,
        total_cow_rewarded / days_since_inception as average_daily_cow,
        total_value_rewarded / days_since_inception as average_daily_value,
        365.0 * total_cow_rewarded / days_since_inception as projected_annual_cow,
        365.0 * total_value_rewarded / days_since_inception as projected_annual_value
    from grand_totals
),

vertical_results as (
    select
        1 as rk,
        'Total' as cow_distributed,
        total_cow_rewarded as cow,
        total_value_rewarded as usd
    from final_results
    union distinct
    select
        2 as rk,
        'Average daily' as cow_distributed,
        average_daily_cow as cow,
        average_daily_value as usd
    from final_results
    union distinct
    select
        3 as rk,
        'Projected Annual' as cow_distributed,
        projected_annual_cow as cow,
        projected_annual_value as usd
    from final_results
)

select
    cow_distributed,
    cow,
    usd
from vertical_results
order by rk

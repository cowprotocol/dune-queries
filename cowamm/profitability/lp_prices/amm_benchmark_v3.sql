with prep as (
    select 
        *,
        reserve0*price0*power(10, -decimals0) + reserve1*price1*power(10,-decimals1) as pool_value,
        lag(price0) over (partition by contract_address order by day asc) as previous_price0,
        lag(price1) over (partition by contract_address order by day asc) as previous_price1
    from dune.cowprotocol.result_amm_lp_infos
)

select
    c.contract_address as cow_amm_contract_address,
    c.day,
    c.token0,
    c.token1,
    weight0,
    weight1,
    c.price0,
    c.price1,
    10000 * c.pool_value / c.lp_reserve * first_value(c.lp_reserve / c.pool_value) over (
        partition by c.contract_address order by c.day asc
    ) as cow_amm_investment_value,
    10000 * o.pool_value / u.lp_reserve * first_value(u.lp_reserve / u.pool_value) over (
        partition by u.contract_address order by u.day asc
    ) as uni_investment_value

    
 
from prep as c
inner join prep as u
    on c.contract_address = cow_amm_contract_address
    and c.day = u.day
where c.protocol = 'cow_amm'
    and u.protocol = 'uni'
    and c.lp_reserve > 0
    and u.lp_reserve > 0
with tuesdays as (
    select cast(date_add('week', n, date '2024-12-31') as timestamp) as end_time
    from unnest(sequence(0, 9999)) as t(n) --noqa: AL05, LT01
    where date_add('week', n, date '2024-12-31') <= current_date
),

cow_prices as (
    select
        date(minute) + interval '1' day as end_time,
        avg(price) as cow_price
    from prices.usd
    where blockchain = 'ethereum' and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
    group by 1
),

native_token_prices as (
    select --noqa: ST06
        date(minute) + interval '1' day as end_time,
        blockchain,
        contract_address,
        avg(price) as native_token_price
    from prices.usd
    where (
        blockchain = 'ethereum' and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        or blockchain = 'gnosis' and contract_address = 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
        or blockchain = 'arbitrum' and contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
        or blockchain = 'base' and contract_address = 0x4200000000000000000000000000000000000006
        or blockchain = 'avalanche_c' and contract_address = 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
        or blockchain = 'polygon' and contract_address = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
    )
    group by 1, 2, 3
)

select
    t.end_time,
    p.blockchain,
    p.native_token_price,
    c.cow_price
from tuesdays as t inner join native_token_prices as p on t.end_time = p.end_time
inner join cow_prices as c on t.end_time = c.end_time

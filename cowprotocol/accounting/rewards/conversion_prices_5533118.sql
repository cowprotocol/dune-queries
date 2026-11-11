-- One row per chain and day; end_time is the day after the averaged day.
-- Tuesday rows are the official accounting period rates.

with cow_prices as (
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
        or blockchain = 'lens' and contract_address = 0x6bdc36e20d267ff0dd6097799f82e78907105e2f
        or blockchain = 'bnb' and contract_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
        or blockchain = 'linea' and contract_address = 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f
        or blockchain = 'plasma' and contract_address = 0x6100e367285b01f48d07953803a2d8dca5d19873
        or blockchain = 'ink' and contract_address = 0x4200000000000000000000000000000000000006
        or blockchain = 'solana' and contract_address = from_base58('So11111111111111111111111111111111111111112')
    )
    group by 1, 2, 3
)

select
    p.end_time,
    p.blockchain,
    p.native_token_price,
    c.cow_price
from native_token_prices as p
inner join cow_prices as c on p.end_time = c.end_time
where p.end_time >= timestamp '2024-12-31'

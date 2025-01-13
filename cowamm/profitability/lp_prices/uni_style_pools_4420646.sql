-- Finds all the Uniswap style pool: Uniswap, Sushiswap, Pancakeswap
-- Parameters
--    {{blockchain}}

select
    block_time as created_at,
    substr(data, 13, 20) as contract_address,
    substr(topic1, 13, 20) as token0,
    substr(topic2, 13, 20) as token1,
    case
        when
            contract_address in (
                0x1097053Fd2ea711dad45caCcc45EfF7548fCB362,
                0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e
            ) then 'pancakeswap'
        when
            contract_address in (
                0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f,
                0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9,
                0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6
            )
            then 'uniswap'
        when
            contract_address in (
                0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac,
                0xc35DADB65012eC5796536bD9864eD8773aBc74C4,
                0x71524B4f93c58fcbF659783284E38825f0622859
            )
            then 'sushiswap'
    end as project
from {{blockchain}}.logs
where
    topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
    and contract_address in
    (
        0x1097053Fd2ea711dad45caCcc45EfF7548fCB362, --eth, pancake
        0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e, --arb/bas, pancake
        0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, --eth, uni
        0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9, --arb, uni
        0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6, --bas, uni
        0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac, --eth, sushi
        0xc35DADB65012eC5796536bD9864eD8773aBc74C4, --arb/gno, sushi
        0x71524B4f93c58fcbF659783284E38825f0622859 --bas, sushi
    )

-- This query parses the raw price_feed table that we sync on Dune,
-- and returns a view that is based on minute prices, quite similar to
-- how the Dune price.usd looks like
--
--
-- The columns of the result are
-- - minute: the minute for which a price is computed
-- - blockchain: the corresponding chain
-- - contract_address: a token
-- - decimals: the decimals of the token
-- - price: the price, in USD, of one unit of the token
-- - source: the source from which this price was computed,

with imported_prices as (
    select
        cast(replace(time, 'T', ' ') as timestamp) as time, --noqa: RF04
        'ethereum' as blockchain,
        token_address as contract_address,
        decimals,
        source,
        cast(price as double) as price_unit_eth
    from dune.cowprotocol.dataset_price_feed_ethereum
),

imported_prices_per_minute as (
    select -- noqa: ST06
        date_trunc('minute', time) as minute, --noqa: RF04
        blockchain,
        contract_address,
        decimals,
        source,
        avg(price_unit_eth) as price_unit_eth
    from imported_prices group by 1, 2, 3, 4, 5
)

select -- noqa: ST06
    ippm.minute,
    ippm.blockchain,
    ippm.contract_address,
    ippm.decimals,
    ippm.source,
    ippm.price_unit_eth * p.price as price
from imported_prices_per_minute as ippm
inner join prices.usd as p on ippm.minute = p.minute
where p.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 and p.blockchain = 'ethereum'
order by ippm.minute desc --noqa: AM06

-- This query parses the raw price_feed table that we sync on Dune,
-- and returns a view that is based on hourly prices, similar to
-- how the Dune price feed is used for slippage accounting
--
-- Parameters:
--  {{blockchain}} - network to run the analysis on; currently it only works for mainnet
--
-- The columns of the result are
-- - token_address: a token
-- - hour: the hour for which a price is computed
-- - decimals: the decimals of the token
-- - source: the source from which this price was computed,
--   price_unit: the price, in USD, of one unit of the token
-- - price_atom: the price, in USD, of one atom of the token

with imported_prices as (
    select
        token_address,
        cast(replace(time, 'T', ' ') as timestamp) as time, --noqa: RF04
        cast(price as double) as price_unit_eth,
        decimals,
        source
    from dune.cowprotocol.dataset_price_feed_{{blockchain}}
),

imported_prices_per_minute as (
    select -- noqa: ST06
        token_address,
        date_trunc('minute', time) as minute, --noqa: RF04
        decimals,
        source,
        avg(price_unit_eth) as price_unit_eth
    from imported_prices group by 1, 2, 3, 4
),

imported_prices_per_minute_with_usd_prices as (
    select -- noqa: ST06
        ippm.token_address,
        ippm.minute,
        ippm.decimals,
        ippm.price_unit_eth,
        ippm.price_unit_eth * p.price as price_unit,
        ippm.price_unit_eth * p.price / pow(10, ippm.decimals) as price_atom,
        source
    from imported_prices_per_minute as ippm inner join prices.usd as p on ippm.minute = p.minute
    where p.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 and blockchain = 'ethereum'
)

select -- noqa: ST06
    token_address,
    date_trunc('hour', minute) as hour, --noqa: RF04
    decimals,
    source,
    avg(price_unit) as price_unit,
    avg(price_atom) as price_atom
from imported_prices_per_minute_with_usd_prices group by 1, 2, 3, 4

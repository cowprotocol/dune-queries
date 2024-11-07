with imported_prices as (
    select
        token_address,
        cast(replace(time, 'T', ' ') as timestamp) as time, --noqa: RF04
        cast(price as double) as price_unit_eth,
        decimals,
        source
    from dune.cowprotocol.dataset_price_feed_ethereum
),

processed_data as (
    select -- noqa: ST06
        token_address,
        date_trunc('minute', time) as minute, --noqa: RF04
        decimals,
        source,
        avg(price_unit_eth) as price_unit_eth
    from imported_prices group by 1, 2, 3, 4
),

processed_data_with_usd_prices as (
    select -- noqa: ST06
        pd.token_address,
        pd.minute,
        pd.decimals,
        pd.price_unit_eth,
        pd.price_unit_eth * p.price as price_unit,
        pd.price_unit_eth * p.price / pow(10, pd.decimals) as price_atom,
        source
    from processed_data as pd inner join prices.usd as p on pd.minute = p.minute
    where p.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 and blockchain = 'ethereum'
)

select -- noqa: ST06
    token_address,
    date_trunc('hour', minute) as hour, --noqa: RF04
    decimals,
    source,
    avg(price_unit) as price_unit,
    avg(price_atom) as price_atom
from processed_data_with_usd_prices group by 1, 2, 3, 4

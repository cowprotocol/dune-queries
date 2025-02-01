# Slippage accounting

The queries in this folder are used for slippage accounting on CoW Protocol.
Results are used for solver payments in https://github.com/cowprotocol/solver-rewards.
Additional information can be found in the [documentation](https://docs.cow.fi/cow-protocol/reference/core/auctions/accounting#slippage).

## Query structure

![Structure of slippage query](slippage.svg)

The figure was generated using https://app.diagrams.net/.

## Notes

- Slippage depends on raw token imbalances as well as information on protocol and network fees.
- Slippage in different tokens is converted to ETH using the Dune price feed `prices.usd`, as implemented in `slippage_prices_4064601.sql`. The price feed uses hourly prices. If a price is _not_ available in the feed, exchange rates from CoW Protocol are used to reconstruct a price. If no price can be reconstructed, settlements are excluded from slippage.
- The file `slippage_query_3427730.sql` is a wrapper to make results from slippage conform to the format used in `solver-rewards`.

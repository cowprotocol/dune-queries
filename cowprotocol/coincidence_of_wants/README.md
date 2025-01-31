# Coincidence of Wants

The queries in this folder are used to compute quantitative measures of Coincidence of Wants (CoW).
Results are used for solver bounties and general monitoring.

The approach to measuring CoWs is based on a [Master's Thesis by Vigan Lladrovci](https://wwwmatthes.in.tum.de/pages/y9xcjv094zhn/Master-s-Thesis-Vigan-Lladrovci).

## Query structure

![Structure of CoW query](cow.svg)

The figure was generated using https://app.diagrams.net/.

## Notes

- This measure of CoWs cannot detect all benefits of batching.
- Slippage is treated as if there had been additional AMM interactions which lead to a settlement without slippage.

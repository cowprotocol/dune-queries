-- This is part of a base query for monitoring Balancer CoW AMMs
-- It is then materialized for reuse in other queries and optimized performance
-- It indexes all Balancer CoW AMMs on ethereum, gnosis, arbitrum and base
--
-- the final table has columns
-- - created_at: the creation timestamp
-- - blockchain: 'ethereum' or 'gnosis' or 'arbitrum' or 'base'
-- - address: address of Balancer CoW AMM
-- - token_1_address: address of token with smaller address
-- - token_2_address: address of token with larger address

select * from "query_4814818(blockchain = 'ethereum')" --noqa: AM04
union all
select * from "query_4814818(blockchain = 'gnosis')"
union all
select * from "query_4814818(blockchain = 'arbitrum')"
union all
select * from "query_4814818(blockchain = 'base')"

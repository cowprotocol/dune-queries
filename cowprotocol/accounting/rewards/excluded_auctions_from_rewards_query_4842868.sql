with

------------------------------
-- this part lists all auctions on Base that happened in the accounting week of
-- March 4-11, 2025, and included either of the following orders:
--   0x3971adfb215a6174115a977b1d8915466699c1bc17c493c548971668b1780653bebe59328bd8b0b2879a6b897592e6190773c09567cfd4b2, or
--   0x3c8a1b22e804935157767226acf931dc7443b5df4d98b4ee1a1a3017c90d0b51711429b3fdf0e76cf7288e8e4078dfdc5366026467d39719
block_range_march_4_11_2025 as (
    select
        min("number") as start_block,
        max("number") as end_block
    from base.blocks
    where time >= cast('2025-03-04 00:00:00' as timestamp) and time < cast('2025-03-11 00:00:00' as timestamp)
),

base_march_4_11_auctions_prelim as (
    select
        'base' as blockchain,
        environment,
        auction_id
    from "query_4364122(blockchain='base')"
    where block_number >= (select start_block from block_range_march_4_11_2025) and block_number <= (select end_block from block_range_march_4_11_2025) and (
        order_uid = 0x3971adfb215a6174115a977b1d8915466699c1bc17c493c548971668b1780653bebe59328bd8b0b2879a6b897592e6190773c09567cfd4b2
        or
        order_uid = 0x3c8a1b22e804935157767226acf931dc7443b5df4d98b4ee1a1a3017c90d0b51711429b3fdf0e76cf7288e8e4078dfdc5366026467d39719
    )
),

base_march_4_11_auctions_final as (
    select
        *,
        0 as multiplier
    from base_march_4_11_auctions_prelim
)
------------------------------

select
    bd.environment,
    bd.auction_id,
    ea.multiplier
from base_march_4_11_auctions_final as ea inner join "query_4351957(blockchain='{{blockchain}}')" as bd on ea.environment = bd.environment and ea.auction_id = bd.auction_id

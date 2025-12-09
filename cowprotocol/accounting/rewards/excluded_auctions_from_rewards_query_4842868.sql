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
),

mainnet_auction_ids as (
    select *
    from unnest (array[
        11774697,
        11774702,
        11774703,
        11774704,
        11774705,
        11774706,
        11774708,
        11774709,
        11774710,
        11774711,
        11774714,
        11774715,
        11774716,
        11774718,
        11774735,
        11774736,
        11774739,
        11774741,
        11774743,
        11774745,
        11774747,
        11774748,
        11774750,
        11774758,
        11774769,
        11774774,
        11774788,
        11774798,
        11774801,
        11775173,
        11775196,
        11775197,
        11775201,
        11775207,
        11775221,
        11775227,
        11775228,
        11775232,
        11775233,
        11775234,
        11775235,
        11775238,
        11775251,
        11775263,
        11775651,
        11775661,
        11775666,
        11775679,
        11775683,
        11775731,
        11775736,
        11775738,
        11775746,
        11775748
    ]) as t(auction_id)
),

mainnet_nov_18_25_2025_auctions as (
    select
        'ethereum' as blockchain,
        'prod' as environment,
        0 as multiplier,
        auction_id
    from mainnet_auction_ids
),

base_auction_ids as (
    select *
    from unnest (array[
        26267414,
        26267417,
        26267418,
        26267420,
        26267421,
        26267423,
        26267424,
        26267426,
        26267427,
        26267428,
        26267429,
        26267430,
        26267431,
        26267432,
        26267433,
        26267434,
        26267435,
        26267436,
        26267437,
        26267438,
        26267439,
        26267441,
        26267442,
        26267443,
        26267444,
        26267445,
        26267446,
        26267449,
        26267450,
        26267452,
        26267453,
        26267454,
        26267455,
        26267456,
        26267459,
        26267460,
        26267462,
        26267463,
        26267464,
        26267468,
        26267469,
        26267474,
        26267485,
        26267487,
        26267488,
        26267489,
        26267492,
        26267493,
        26267495,
        26267497,
        26267500,
        26267503,
        26267504,
        26267505
    ]) as t(auction_id)
),

base_nov25_dec2_2025_auctions as (
    select
        'base' as blockchain,
        'prod' as environment,
        0 as multiplier,
        auction_id
    from base_auction_ids
),

mainnet_auctions_ids_dec2_9_2025 as (
    select *
    from unnest (array[
        11854671
        ,11854676
        ,11854678
        ,11854680
        ,11854681
        ,11854693
        ,11854694
        ,11854696
        ,11854703
        ,11854704
        ,11854705
        ,11854708
        ,11854709
        ,11854711
        ,11854713
        ,11854716
        ,11854717
        ,11854722
        ,11854736
        ,11854740
        ,11854749
        ,11854753
        ,11854754
        ,11854756
        ,11854757
        ,11854758
        ,11854759
        ,11854760
        ,11854761
        ,11854762
        ,11854763
        ,11854764
        ,11854765
        ,11854766
        ,11854767
        ,11854768
        ,11854770
        ,11854771
        ,11854772
        ,11854773
        ,11854774
        ,11854775
        ,11854776
        ,11854777
        ,11854779
        ,11854780
        ,11854781
        ,11854782
        ,11854784
        ,11854785
        ,11854786
        ,11854787
        ,11854788
        ,11854789
        ,11854795
        ,11854802
        ,11854803
        ,11854804
        ,11854805
        ,11854808
        ,11854809
        ,11854810
        ,11854811
        ,11854812
        ,11854813
        ,11854814
        ,11854815
        ,11854816
        ,11854817
        ,11854818
        ,11854819
        ,11854820
        ,11854821
        ,11854822
        ,11854823
        ,11854824
        ,11854825
        ,11854826
        ,11854827
        ,11854828
        ,11854829
        ,11854830
        ,11854831
        ,11854832
        ,11854833
        ,11854834
        ,11854835
        ,11854836
        ,11854837
        ,11854838
        ,11854839
        ,11854840
        ,11854841
        ,11854842
        ,11854843
        ,11854846
        ,11854847
        ,11854848
        ,11854849
        ,11854850
        ,11854851
        ,11854876
        ,11854878
        ,11854879
        ,11854880
        ,11854881
        ,11854882
        ,11854884
        ,11854885
        ,11854886
        ,11854887
        ,11854888
        ,11854889
        ,11854890
        ,11854893
        ,11854895
        ,11854896
        ,11854897
        ,11854901
        ,11854914
        ,11854916
        ,11854917
        ,11854918
        ,11854919
        ,11854920
        ,11854922
        ,11854924
        ,11854925
        ,11854926
        ,11854927
        ,11854929
        ,11854930
        ,11854931
        ,11854932
        ,11854933
        ,11854934
        ,11854935
        ,11854937
        ,11854938
        ,11854946
        ,11854947
        ,11854948
        ,11854949
        ,11854952
        ,11854953
        ,11854954
        ,11854955
        ,11854956
        ,11854957
        ,11854959
        ,11854960
        ,11854961
        ,11854962
        ,11854963
        ,11854964
        ,11854965
        ,11854969
        ,11854979
        ,11854987
        ,11854995
        ,11854997
        ,11855003
        ,11855005
        ,11855006
        ,11855007
        ,11855008
        ,11855009
        ,11855010
        ,11855016
        ,11855018
        ,11855019
        ,11855020
        ,11855021
        ,11855022
        ,11855023
        ,11855024
        ,11855025
        ,11855027
        ,11855029
        ,11855030
        ,11855031
        ,11855032
        ,11855033
        ,11855035
        ,11855036
        ,11855037
        ,11855038
        ,11855079
        ,11855080
        ,11855082
        ,11855095
        ,11855135
        ,11855137
        ,11855138
        ,11855195
        ,11855238
        ,11855239
        ,11855242
        ,11855245
        ,11855246
        ,11855251
        ,11855282
        ,11855337
    ]) as t(auction_id)
),

ethereum_dec2_9_2025_auctions as (
    select
        'ethereum' as blockchain,
        'prod' as environment,
        0 as multiplier,
        auction_id
    from mainnet_auctions_ids_dec2_9_2025
)


------------------------------

-- select
--     bd.environment,
--     bd.auction_id,
--     ea.multiplier
-- from base_march_4_11_auctions_final as ea inner join "query_4351957(blockchain='{{blockchain}}')" as bd on ea.environment = bd.environment and ea.auction_id = bd.auction_id
-- union all
select distinct
    bd.environment,
    bd.auction_id,
    m.multiplier
from mainnet_nov_18_25_2025_auctions as m inner join "query_4351957(blockchain='{{blockchain}}')" as bd on m.environment = bd.environment and m.auction_id = bd.auction_id
where m.blockchain = '{{blockchain}}'
union all
select distinct
    bd.environment,
    bd.auction_id,
    m.multiplier
from base_nov25_dec2_2025_auctions as m inner join "query_4351957(blockchain='{{blockchain}}')" as bd on m.environment = bd.environment and m.auction_id = bd.auction_id
where m.blockchain = '{{blockchain}}'
union all
select distinct
    bd.environment,
    bd.auction_id,
    m.multiplier
from ethereum_dec2_9_2025_auctions as m inner join "query_4351957(blockchain='{{blockchain}}')" as bd on m.environment = bd.environment and m.auction_id = bd.auction_id

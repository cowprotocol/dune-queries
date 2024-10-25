-- Query that hardcodes all existing full bonding pools that
--  are currently valid at CoW Protocol.
with
full_bonding_pools as (
    select
        from_hex('0x8353713b6D2F728Ed763a04B886B16aAD2b16eBD') as pool_address,
        'Gnosis' as pool_name,
        from_hex('0x6c642cafcbd9d8383250bb25f67ae409147f78b2') as initial_funder
    union distinct
    select
        from_hex('0x5d4020b9261F01B6f8a45db929704b0Ad6F5e9E6') as pool_address,
        'CoW DAO' as pool_name,
        from_hex('0x423cec87f19f0778f549846e0801ee267a917935') as initial_funder
    union distinct
    select
        from_hex('0xC96569Dc132ebB6694A5f0b781B33f202Da8AcE8') as pool_address,
        'Project Blanc' as pool_name,
        from_hex('0xCa99e3Fc7B51167eaC363a3Af8C9A185852D1622') as initial_funder
    union distinct
    select
        from_hex('0x0deb0ae9c4399c51289adb1f3ed83557a56df657') as pool_address,
        'Rizzolver' as pool_name,
        from_hex('0x607DBe787C242e20A4897680357336E37954b3F0') as initial_funder
    union distinct
    select
        from_hex('0x3075F6aab29D92F8F062A83A0318c52c16E69a60') as pool_address,
        'Furucombo' as pool_name,
        from_hex('0x539B3640c26D9159c9Ea9B7e2a1aa69D89Db6ee1') as initial_funder
)

select *
from
    full_bonding_pools

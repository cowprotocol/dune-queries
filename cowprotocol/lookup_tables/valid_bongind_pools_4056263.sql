-- Query that hardcodes all existing full bonding pools that
--  are currently valid at CoW Protocol.
with
full_bonding_pools as (
    select
        from_hex('0x8353713b6D2F728Ed763a04B886B16aAD2b16eBD') as pool_address, -- deprecated
        'Gnosis' as pool_name,
        from_hex('0x6c642cafcbd9d8383250bb25f67ae409147f78b2') as creator
    union distinct
    select
        from_hex('0x5d4020b9261F01B6f8a45db929704b0Ad6F5e9E6') as pool_address,
        'CoW DAO' as pool_name,
        from_hex('0x423cec87f19f0778f549846e0801ee267a917935') as creator
    union distinct
    select
        from_hex('0xC96569Dc132ebB6694A5f0b781B33f202Da8AcE8') as pool_address, -- deprecated
        'Project Blanc' as pool_name,
        from_hex('0xCa99e3Fc7B51167eaC363a3Af8C9A185852D1622') as creator
    union distinct
    select
        from_hex('0x7489f267C3b43dc76e4cb190F7B55ab3297706AF') as pool_address,
        'Gnosis DAO' as pool_name,
        from_hex('0x717e745040b9a486f2254659E8EA7Dc7d9a72A1e') as creator
    union distinct
    select
        from_hex('0xe78d5F3aba2B31C980bF5E35E05B3A55b8365b48') as pool_address,
        'Project Blanc' as pool_name,
        from_hex('0xCa99e3Fc7B51167eaC363a3Af8C9A185852D1622') as creator
    union distinct
    select
        from_hex('0x0deb0ae9c4399c51289adb1f3ed83557a56df657') as pool_address,
        'Rizzolver' as pool_name,
        from_hex('0x042c9c6d52881dc7e70bf3e233b540a07377d26b') as creator
    union distinct
    select
        from_hex('0x7719c9c0d35d460b00487a1744394e9525e8a42c') as pool_address,
        'Fractal' as pool_name,
        from_hex('0xd4676b4de3a982a429a8dbe90d4a7e7cfb4769a5') as creator
)

select *
from full_bonding_pools

-- Query that hardcodes all existing full bonding pools that are currently valid at CoW Protocol.

with full_bonding_pools as (
    select *
    from (values
        (from_hex('0x8353713b6d2f728ed763a04b886b16aad2b16ebd'), 'Gnosis',        from_hex('0x6c642cafcbd9d8383250bb25f67ae409147f78b2')), -- deprecated
        (from_hex('0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6'), 'CoW DAO',       from_hex('0x423cec87f19f0778f549846e0801ee267a917935')),
        (from_hex('0xc96569dc132ebb6694a5f0b781b33f202da8ace8'), 'Project Blanc', from_hex('0xca99e3fc7b51167eac363a3af8c9a185852d1622')), -- deprecated
        (from_hex('0x7489f267c3b43dc76e4cb190f7b55ab3297706af'), 'Gnosis DAO',    from_hex('0x717e745040b9a486f2254659e8ea7dc7d9a72a1e')),
        (from_hex('0xe78d5f3aba2b31c980bf5e35e05b3a55b8365b48'), 'Project Blanc', from_hex('0xca99e3fc7b51167eac363a3af8c9a185852d1622')),
        (from_hex('0x0deb0ae9c4399c51289adb1f3ed83557a56df657'), 'Rizzolver',     from_hex('0x042c9c6d52881dc7e70bf3e233b540a07377d26b')),
        (from_hex('0x7719c9c0d35d460b00487a1744394e9525e8a42c'), 'Fractal',       from_hex('0xd4676b4de3a982a429a8dbe90d4a7e7cfb4769a5'))
    ) as t(pool_address, pool_name, creator)
)
select *
from full_bonding_pools;

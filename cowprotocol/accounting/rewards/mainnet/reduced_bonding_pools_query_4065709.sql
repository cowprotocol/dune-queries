-- Query that hardcodes all existing reduced CoW DAO bonding pools that
--  are currently valid at CoW Protocol.
with
reduced_bonding_pools as (
    select
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool_address,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xC7899Ff6A3aC2FF59261bD960A8C880DF06E1041') as solver_address, -- prod-Barter
        timestamp '2024-08-21 07:15:00' as creation_date
    union distinct
    select
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool_address,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xA6A871b612bCE899b1CbBad6E545e5e47Da98b87') as solver_address,  -- barn-Barter
        timestamp '2024-08-21 07:15:00' as creation_date
    union distinct
    select
        from_hex('0xc5Dc06423f2dB1B11611509A5814dD1b242268dd') as pool_address,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0x008300082C3000009e63680088f8c7f4D3ff2E87') as solver,  -- prod-Copium_Capital
        timestamp '2024-07-25 07:42:00' as creation_date
)

select *
from
    reduced_bonding_pools

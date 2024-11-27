-- Query that hardcodes all existing reduced CoW DAO bonding pools that
--  are currently valid at CoW Protocol.
with
reduced_bonding_pools as (
    select
        'prod-Barter' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool_address,
        from_hex('0xC7899Ff6A3aC2FF59261bD960A8C880DF06E1041') as solver_address,
        timestamp '2024-08-21 07:15:00' as creation_date
    union distinct
    select
        'barn-Barter' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xB6113c260aD0a8A086f1E31c5C92455252A53Fb8') as pool_address,
        from_hex('0xA6A871b612bCE899b1CbBad6E545e5e47Da98b87') as solver_address,
        timestamp '2024-08-21 07:15:00' as creation_date
    union distinct
    select
        'prod-Copium_Capital' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xc5Dc06423f2dB1B11611509A5814dD1b242268dd') as pool_address,
        from_hex('0x008300082C3000009e63680088f8c7f4D3ff2E87') as solver_address,
        timestamp '2024-07-25 07:42:00' as creation_date
    union distinct
    select
        'prod-Rizzolver' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0x0Deb0Ae9c4399C51289adB1f3ED83557A56dF657') as pool_address,
        from_hex('0x9DFc9Bb0FfF2dc96728D2bb94eaCee6ba3592351') as solver_address,
        timestamp '2024-10-10 02:03:00' as creation_date
    union distinct
    select
        'barn-Rizzolver' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0x0deb0ae9c4399c51289adb1f3ed83557a56df657') as pool_address,
        from_hex('0x26B5e3bF135D3Dd05A220508dD61f25BF1A47cBD') as solver_address,
        timestamp '2024-10-10 02:03:00' as creation_date
    union distinct
    select
        'prod-Portus' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0x3075F6aab29D92F8F062A83A0318c52c16E69a60') as pool_address,
        from_hex('0x6bf97aFe2D2C790999cDEd2a8523009eB8a0823f') as solver_address,
        timestamp '2024-10-21 03:33:00' as creation_date
    union distinct
    select
        'barn-Portus' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0x3075F6aab29D92F8F062A83A0318c52c16E69a60') as pool_address,
        from_hex('0x5131590ca2E9D3edC182581352b289dcaE83430c') as solver_address,
        timestamp '2024-10-21 03:33:00' as creation_date
    union distinct
    select
        'prod-Fractal' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xDdb0a7BeBF71Fb5d3D7FB9B9B0804beDdf9C1C88') as pool_address,
        from_hex('0x95480d3f27658e73b2785d30beb0c847d78294c7') as solver_address,
        timestamp '2024-10-29 11:57:00' as creation_date
    union distinct
    select
        'barn-Fractal' as solver_name,
        'Reduced-CoW-DAO' as pool_name,
        from_hex('0xDdb0a7BeBF71Fb5d3D7FB9B9B0804beDdf9C1C88') as pool_address,
        from_hex('0x2a2883ade8ce179265f12fc7b48a4b50b092f1fd') as solver_address,
        timestamp '2024-10-29 11:57:00' as creation_date
)

select *
from
    reduced_bonding_pools

-- This is a base query for monitoring balance changes on CoW Protocol
--
-- The query collects all balance changes to the settlement contract. Those changes can come from
-- - erc20 transfers
-- - native transfers
-- - chain specific event like deposits and withdrawals
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are:
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - token_address: address of token with a balance change. contract address for erc20 tokens,
--   0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee for native token
-- - sender: origin of transfers sending tokens to the settlement contract,
--   0x0000000000000000000000000000000000000000 for deposits/withdrawals
-- - receiver: destination of transfer sending tokens from the settlement contract,
--   0x0000000000000000000000000000000000000000 for deposits/withdrawals
-- - amount: value of the balance change in atoms of the token

-- 1) data on all chains
-- 1.1) erc20
-- 1.2) native transfers

-- 1.1) all the erc20 transfers to/from cow amms
with erc20_transfers as (
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        "from" as sender,
        to as receiver,
        value as amount
    from erc20_{{blockchain}}.evt_transfer
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in ("from", to)
        -- the conditions that follow are only needed for Lens, where for some reason the native token
        -- transfers, captured in the 0x000000000000000000000000000000000000800a address, are picked in
        -- the erc20_lens.evt_transfer table.
        -- As we are handling them later on in the same way as we do with all other chains, we decided to
        -- filter them out from this cte
        and (contract_address != 0x000000000000000000000000000000000000800a or '{{blockchain}}' != 'lens')
),

-- 1.2) all native token transfers
native_transfers as (
    select
        block_time,
        tx_hash,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
        "from" as sender,
        to as receiver,
        value as amount
    from {{blockchain}}.traces
    where
        block_time >= cast('{{start_time}}' as timestamp)
        and block_time < cast('{{end_time}}' as timestamp)
        and value > cast(0 as uint256) --noqa: PRS, LT02
        and success = true
        and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
),

-- 2) chain specific data
-- 2.1) ethereum
-- special treatment of
-- 2.1.1) WETH
-- 2.1.2) sDAI
-- 2.1.3) MKR

-- 2.1.1) all deposit and withdrawal events for WETH
weth_deposits_withdrawals_ethereum as (
    -- deposits (contract deposits ETH to get WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from zeroex_ethereum.WETH9_evt_Deposit --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from zeroex_ethereum.WETH9_evt_Withdrawal  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

-- 2.1.2) all deposit and withdrawal events for sDAI
sdai_deposits_withdraws_ethereum as (
    -- deposits
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        shares as amount
    from maker_ethereum.SavingsDai_evt_Deposit  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdraws
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        shares as amount
    from maker_ethereum.SavingsDai_evt_Withdraw  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

-- 2.1.3) all mint and burn events for MKR
mkr_mint_burn_ethereum as (
    -- deposits
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from maker_ethereum.mkr_evt_Mint  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and guy = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdraws
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from maker_ethereum.mkr_evt_Burn  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and guy = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_ethereum as (
    select * from weth_deposits_withdrawals_ethereum
    union all
    select * from sdai_deposits_withdraws_ethereum
    union all
    select * from mkr_mint_burn_ethereum
),

-- 2.2) gnosis
-- special treatment of
-- 2.2.1) WXDAI

-- 2.2.1) all deposit and withdrawal events for WXDAI
wxdai_deposits_withdrawals_gnosis as (
    -- deposits (contract deposits XDAI to get WXDAI)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from wxdai_gnosis.WXDAI_evt_Deposit  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws XDAI by returning WXDAI)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from wxdai_gnosis.WXDAI_evt_Withdrawal  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_gnosis as ( -- noqa: ST03
    select * from wxdai_deposits_withdrawals_gnosis
),

-- 2.3) arbitrum
-- special treatment of
-- 2.3.1) WETH

-- 2.3.1) all deposit and withdrawal events for WETH
weth_deposits_withdrawals_arbitrum as (
    -- deposits (contract deposits ETH to get WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from mindgames_weth_arbitrum.WETH9_evt_Deposit  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from mindgames_weth_arbitrum.WETH9_evt_Withdrawal  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_arbitrum as ( -- noqa: ST03
    select * from weth_deposits_withdrawals_arbitrum
),

-- 2.4) base
-- special treatment of
-- 2.4.1) WETH

-- 2.4.1) all deposit and withdrawal events for WETH
weth_deposits_withdrawals_base as (
    -- deposits (contract deposits ETH to get WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from weth_base.WETH9_evt_Deposit  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from weth_base.WETH9_evt_Withdrawal  --noqa: CP02
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_base as ( -- noqa: ST03
    select * from weth_deposits_withdrawals_base
),

-- 2.5) avalanche
-- special treatment of
-- 2.5.1) WAVAX

-- 2.5.1) all deposit and withdrawal events for WAVAX
wavax_deposits_withdrawals_avalanche_c as (
    -- deposits (contract deposits AVAX to get WAVAX)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from wavax_avalanche_c.wavax_evt_deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws AVAX by returning WAVAX)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from wavax_avalanche_c.wavax_evt_withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_avalanche_c as ( -- noqa: ST03
    select * from wavax_deposits_withdrawals_avalanche_c
),

-- 2.6) polygon
-- special treatment of
-- 2.6.1) WPOL

-- 2.6.1) all deposit and withdrawal events for WPOL
wpol_deposits_withdrawals_polygon as (
    -- deposits (contract deposits POL to get WPOL)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from mahadao_polygon.wmatic_evt_deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws POL by returning WPOL)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from mahadao_polygon.wmatic_evt_withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_polygon as ( -- noqa: ST03
    select * from wpol_deposits_withdrawals_polygon
),

-- 2.7) lens
-- special treatment of
-- 2.7.1) WGHO

-- 2.7.1) all deposit and withdrawal events for WGHO
wgho_all_deposits_withdrawals_lens as (
-- WGHO deposits & withdrawals on Lens
    select
        block_time,
        block_number,
        tx_hash,
        contract_address,
        topic0,
        from_hex(substr(cast(topic1 as varchar), 27)) as src_dst_address, -- indexed address (dst for Deposit, src for Withdrawal)
        varbinary_to_uint256(data) as wad
    from lens.logs
    where contract_address = 0x6bdc36e20d267ff0dd6097799f82e78907105e2f and (
        topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c -- Deposit(address,uint256)
        or
        topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65  -- Withdrawal(address,uint256)
    )
),

wgho_deposits_withdrawals_lens as (
    -- deposits (contract deposits GHO to get WGHO)
    select
        block_time,
        tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from wgho_all_deposits_withdrawals_lens
    where
        block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp) -- partition column
        and topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
        and src_dst_address = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws GHO by returning WGHO)
    select
        block_time,
        tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from wgho_all_deposits_withdrawals_lens
    where
        block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp) -- partition column
        and topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65 
        and src_dst_address = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_lens as ( -- noqa: ST03
    select * from wgho_deposits_withdrawals_lens
),

-- 2.8) bnb
-- special treatment of
-- 2.8.1) WBNB

-- 2.8.1) all deposit and withdrawal events for WBNB
wbnb_deposits_withdrawals_bnb as (
    -- deposits (contract deposits BNB to get WBNB)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from bnb_bnb.wbnb_evt_deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws BNB by returning WBNB)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from bnb_bnb.wbnb_evt_withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_bnb as ( -- noqa: ST03
    select * from wbnb_deposits_withdrawals_bnb
),

-- 2.9) linea
-- special treatment of
-- 2.9.1) WETH

-- 2.9.1) all deposit and withdrawal events for WETH
weth_deposits_withdrawals_linea as (
    -- deposits (contract deposits ETH to get WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from linea_linea.weth9_evt_deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from linea_linea.weth9_evt_withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_linea as ( -- noqa: ST03
    select * from weth_deposits_withdrawals_linea
),

-- 2.10) plasma
-- special treatment of
-- 2.10.1) WXPL

-- 2.10.1) all deposit and withdrawal events for WXPL
wxpl_deposits_withdrawals_plasma as (
    -- deposits (contract deposits ETH to get WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        wad as amount
    from XXXXX
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from XXXXX
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_plasma as ( -- noqa: ST03
    select * from wxpl_deposits_withdrawals_plasma
)


-- combine results
select * from erc20_transfers
union all
select * from native_transfers
union all
select * from special_balance_changes_{{blockchain}}

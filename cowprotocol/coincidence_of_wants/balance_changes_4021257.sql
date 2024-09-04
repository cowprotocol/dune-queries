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
        and 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 in ("from", to)
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
        block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp) -- partition column
        and value > cast(0 as uint256)
        and success = true
        and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
),

-- 2) chain specific data
-- 2.1) ethereum
-- special treatmet of
-- 2.1.1) WETH
-- 2.1.2) sDAI

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
    from zeroex_ethereum.WETH9_evt_Deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union distinct
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from zeroex_ethereum.WETH9_evt_Withdrawal
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
        shares as amount_wei
    from maker_ethereum.SavingsDai_evt_Deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union distinct
    -- withdraws
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        shares as amount_wei
    from maker_ethereum.SavingsDai_evt_Withdraw
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_ethereum as (
    select * from weth_deposits_withdrawals_ethereum
    union all
    select * from sdai_deposits_withdraws_ethereum
),

-- 2.2) gnosis
-- special treatmet of
-- 2.2.1) WXDAI
-- 2.2.2) sDAI

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
    from wxdai_gnosis.WXDAI_evt_Deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union distinct
    -- withdrawals (contract withdraws XDAI by returning WXDAI)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from wxdai_gnosis.WXDAI_evt_Withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

-- 2.2.2) all deposit and withdrawal events for sDAI
sdai_deposits_withdraws_gnosis as (
    -- deposits
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        shares as amount_wei
    from sdai_gnosis.SavingsXDai_evt_Deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union distinct
    -- withdraws
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        shares as amount_wei
    from sdai_gnosis.SavingsXDai_evt_Withdraw
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_gnosis as ( -- noqa: ST03
    select * from wxdai_deposits_withdrawals_gnosis
    union all
    select * from sdai_deposits_withdraws_gnosis
),

-- 2.3) arbitrum
-- special treatmet of
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
    from mindgames_weth_arbitrum.WETH9_evt_Deposit
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and dst = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union distinct
    -- withdrawals (contract withdraws ETH by returning WETH)
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        contract_address as token_address,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        wad as amount
    from mindgames_weth_arbitrum.WETH9_evt_Withdrawal
    where
        evt_block_time >= cast('{{start_time}}' as timestamp) and evt_block_time < cast('{{end_time}}' as timestamp) -- partition column
        and src = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

special_balance_changes_arbitrum as ( -- noqa: ST03
    select * from weth_deposits_withdrawals_arbitrum
)

-- combine results
select * from erc20_transfers
union all
select * from native_transfers
union all
select * from special_balance_changes_{{blockchain}}

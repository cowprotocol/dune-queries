select 'ethereum' as blockchain, 0x63695eee2c3141bde314c5a6f89b98e62808d716 as partner_recipient, 'CoW Swap-SafeApp' as app_code_to_excl, 0.9 as partner_share  -- noqa: disable=all
union all select 'ethereum', 0x352a3666b27bb09aca7b4a71ed624429b7549551, '_', 0.85
union all select 'ethereum', 0xe37da2d07e769b7fcb808bdeaeffb84561ff4eca, '_', 0.85
union all select 'ethereum', 0x90a48d5cf7343b08da12e067680b4c6dbfe551be, '_', 0.85
union all select 'ethereum', 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda, '_', 0.85
union all select 'ethereum', 0xe344241493d573428076c022835856a221db3e26, '_', 0.85
union all select 'ethereum', 0x8025bacf968aa82bdfe51b513123b55bfb0060d3, '_', 0.45
union all select 'ethereum', 0xe423c63e8a25811c9cbe71c8585c4505117397c6, '_', 0.75
union all select 'ethereum', 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb, '_', 0.75
union all select 'ethereum', 0xc542c2f197c4939154017c802b0583c596438380, '_', 0.875

union all select 'gnosis', 0x63695eee2c3141bde314c5a6f89b98e62808d716, 'CoW Swap-SafeApp', 0.9
union all select 'gnosis', 0x352a3666b27bb09aca7b4a71ed624429b7549551, '_', 0.85
union all select 'gnosis', 0x8387fae9951724c00c753797b22b897111750673, '_', 0.85
union all select 'gnosis', 0xb0E3175341794D1dc8E5F02a02F9D26989EbedB3, '_', 0.85
union all select 'gnosis', 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda, '_', 0.85
union all select 'gnosis', 0xe344241493d573428076c022835856a221db3e26, '_', 0.85
union all select 'gnosis', 0x8025bacf968aa82bdfe51b513123b55bfb0060d3, '_', 0.45
union all select 'gnosis', 0xe423c63e8a25811c9cbe71c8585c4505117397c6, '_', 0.75
union all select 'gnosis', 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb, '_', 0.75
union all select 'gnosis', 0xc542c2f197c4939154017c802b0583c596438380, '_', 0.875

union all select 'arbitrum', 0x63695eee2c3141bde314c5a6f89b98e62808d716, 'CoW Swap-SafeApp', 0.9
union all select 'arbitrum', 0x352a3666b27bb09aca7b4a71ed624429b7549551, '_', 0.85
union all select 'arbitrum', 0x86cd2bBC859E797B75D86E6eEEC1a726A9284c23, '_', 0.85
union all select 'arbitrum', 0x38276553F8fbf2A027D901F8be45f00373d8Dd48, '_', 0.85
union all select 'arbitrum', 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda, '_', 0.85
union all select 'arbitrum', 0xe344241493d573428076c022835856a221db3e26, '_', 0.85
union all select 'arbitrum', 0x8025bacf968aa82bdfe51b513123b55bfb0060d3, '_', 0.45
union all select 'arbitrum', 0xe423c63e8a25811c9cbe71c8585c4505117397c6, '_', 0.75
union all select 'arbitrum', 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb, '_', 0.75
union all select 'arbitrum', 0xc542c2f197c4939154017c802b0583c596438380, '_', 0.875

union all select 'base', 0x63695eee2c3141bde314c5a6f89b98e62808d716, 'CoW Swap-SafeApp', 0.9
union all select 'base', 0x352a3666b27bb09aca7b4a71ed624429b7549551, '_', 0.85
union all select 'base', 0xAf1c727B605530AcDb00906a158E817f41aFD778, '_', 0.85
union all select 'base', 0x9c9aA90363630d4ab1D9dbF416cc3BBC8d3Ed502, '_', 0.85
union all select 'base', 0xcd777a10502256db93c2b0a8e8f64a5174c6cbda, '_', 0.85
union all select 'base', 0xe344241493d573428076c022835856a221db3e26, '_', 0.85
union all select 'base', 0x8025bacf968aa82bdfe51b513123b55bfb0060d3, '_', 0.45
union all select 'base', 0xe423c63e8a25811c9cbe71c8585c4505117397c6, '_', 0.75
union all select 'base', 0x1713b79e3dbb8a76d80e038ca701a4a781ac69eb, '_', 0.75
union all select 'base', 0xc542c2f197c4939154017c802b0583c596438380, '_', 0.875

/*
example of usage:
select coalesce(partner_fee_splits.partner_share, 0.5) * partner_fee * native_token_price/1e18 as partner_share_of_partner_fee
...
left join "query_5333695" as partner_fee_splits
   on partner_fee_shares.blockchain = <<blockchain parameter>>
   and app_data.partner_recipient = partner_fee_splits.partner_recipient 
   and app_data.app_code != partner_fee_splits.app_code_to_excl
*/

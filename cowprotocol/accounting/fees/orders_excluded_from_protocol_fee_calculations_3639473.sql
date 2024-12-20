-- This query returns a collection of mainnet order uids for which
-- the protocol fee collected'), when denominated in eth'), is wrongly computed'),
-- due to erroneous native prices that were included in the auction.

with excluded_orders as (
    select column1 as order_uid
    from (
        values
        ('0x05a561527ed7128e3cabf8bd76f4c8eb7be124b5ed26efe3245f1d4852a0e0c25f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152a31'),
        ('0x060bc0f9ed708648c6cc8a2443f2f77391912a137db5592dad7e900b9b9f110c5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152b60'),
        ('0x0718467d80fbc5fe51bb4ca7598a1cf6163c2b9bf81a50ff117fbc952844560d6596c1be01cb7733c37f0d3af73e728314d847ec66192f23'),
        ('0x132d5917291140929e700030696d2d97165dffccdba40ad3050e018fe7d015335f0484839862aa8f7b99d1b9faaa3ba6c8ac35a96616d1aa'),
        ('0x1790f9b3348c502144732066518cb6b1e9ddd1650235dbf24846678b38f009fd5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152972'),
        ('0x1ae957bda59f8a814d0f4bb49490688e1a211e4e6d0b1d0d38a655397c337acf5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152b07'),
        ('0x2a23a6ceb5d8e484de8271cdd3c55e022a18b06d1e5935e54e83991f64c1f30e5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152911'),
        ('0x2bf04b014794e4ee5366af289473f14ab9688d1eee46f514f6aaf807e9f228275f0484839862aa8f7b99d1b9faaa3ba6c8ac35a9661530e0'),
        ('0x2c97d87448d366dc3078b1459e09ac809f90afa10b69cd66faa64e30e90bed50ba73d462b61c25cb256128401a74120340e23f88661c5c55'),
        ('0x3183546a5fc9b6aa8d0215da713f84c95ea86d4fcbf9cb1cb120dc35ae479c1d40a50cf069e992aa4536211b23f286ef88752187ffffffff'),
        ('0x3dce8a2948b1dbce51cd4aaf8be8bbc2550fa0af5043f7505947eff731baabe8984a7f1bfc90fe478602c6dfc8e9db9b48e16697661511b7'),
        ('0x5949aec9be0dd2d07efcd99eecd46386b01bf156964ec34a94f5a3063cbe7bca5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152852'),
        ('0x61fbce0d86e591251edc1f99ac74ca1e77f442c51f7ab868881bdd40ae1fcf1e4c691082734142126171893d124db4314f1083e56617963c'),
        ('0x7c2a7fb5128f461c89f34fcce099ddfc72cb0480eaf78f3cb1dc67a63fe59b21984a7f1bfc90fe478602c6dfc8e9db9b48e1669766151184'),
        ('0x7de728465d6bc7dd417c9dc47e90558967f0ad5200a77be371bca85a03fd648e5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a96615283d'),
        ('0x904d6e68cf320952aebd452f5e0709d6eb52ef61503553c03778c61ece423b6b5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a96616d11d'),
        ('0x93ab09b172ee7be204be6e0a01313c1c87a37e4f3c6b8b18fb7812dfa4d464745f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152881'),
        ('0x95a5cb33cf885cfff37f4fb2fc3dd39c974b6bc561115534ffdbbc99994cd3235f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152b82'),
        ('0xa8922239e72372e912b1e28aec72f2090f331f2d500791afc85d094d89467b285f0484839862aa8f7b99d1b9faaa3ba6c8ac35a9661525be'),
        ('0xba3853486660e988702a4dc7d26886515323a047081efa2344589616cb146f825f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152b1f'),
        ('0xc647c87df69f72f9103b9c2b796a6bb621dcfcabf7063cff85500175832177e05f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152ad5'),
        ('0xd045f434af2334201a9787da72604f3b718f60e6edb79f6c3b24fdf7d6b932fe4c691082734142126171893d124db4314f1083e566179be4'),
        ('0xe15fa972932a002c76a9f67fa985908ff9a547740cea4614a0eb286ab7cae1a95f0484839862aa8f7b99d1b9faaa3ba6c8ac35a966152a90'),
        ('0xf617a17a3bd3d6b294819a1e6f313d2891919c4d3a3a143f3d54c294e29be92c5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a9661529d1'),
        ('0x07e9ce836d47e090e32787e6697797d64cb193784f21509f70c5db7935b49d3440a50cf069e992aa4536211b23f286ef88752187ffffffff'),
        ('0x48a4a895a9f680810896221a0b172ee381086b403f0fbf51450db56db28bdd5e5f0484839862aa8f7b99d1b9faaa3ba6c8ac35a9661528c4'),
        ('0x7cd8a08ef53affca762401b04e4a1c95d5c429568a29ae38b6f6fa02e66f8146d16f39990092033ee2fab400bf3364379c616c4e6616cfa5'),
        ('0xd6dda5a9dc263af80b6b4155d61f3cd172432fb0e3564fefa537f90603aea78bffff8298631efa764238485543fcff82b878ce1e66fcdfc0'),
        ('0xaac632d610862392fc638acf8d37734a5c201a0dadb6188ef89e1b2125210d11355f4d611eeb3770933c26f0213f7843350c90f866310ce7'),
        ('0x71fce88cca04bc6eb7e49ce6ea8f830b537fba8703ec37142ef196eb1503cfd515b9b44f2a46154f979afe414646c07fa375928d6632a24d'),
        ('0x1f7065e86d9371e4bc38b484956612194f9da37288daf2f11c6c75e79b5af21b7ac6b7869be51990cd6cf726ef44e821dede7b12663413fc'),
        ('0xa6c265125ff89f1ef1f8834e4ede31fdb1e70746f96cb0963c40dae4c75b904240a50cf069e992aa4536211b23f286ef88752187ffffffff'),
        ('0x743cfe0dd6ac9b9d59556d413ab029d75a1370ea99520cbb358b6e0f6d072eb5272f697ffde25f7c412168986993aa9367d8f716663a81a6'),
        ('0x7cdd4ff6873679767a14c700fa7c542886f17bdd6f1a577e84120863f8b56ded40a50cf069e992aa4536211b23f286ef88752187ffffffff'),
        ('0x4baa716e083a1bea4ea870b44d5e6eef828848e4429eb16e7f96d9c5b1c8107340a50cf069e992aa4536211b23f286ef88752187ffffffff'),
        ('0x9157819168397f2b6450f84a345e048712825aeae163b46a764e0cde7bdf2162b0acb0e8133f1bf5615a46ffad35ba1ccf55c50e665dae15'),
        ('0x942e55be89314c5e799a12b487387a1221e5671bd17222a1bc69cb2b7728a4eea53a13a80d72a855481de5211e7654fabdfe352666e32aec')
    )
)

select order_uid
from excluded_orders

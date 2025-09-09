-- Exclude the following txs/trades from slippage accounting due to wrong Dune prices

select distinct tx_hash
from
    cow_protocol_ethereum.trades
where
    0xf5d669627376ebd411e34b98f19c868c8aba5ada in (buy_token_address, sell_token_address) -- exclude AXS (Old)
    -- mixed ERC20/ERC721 tokens:
    or 0xf66434c34f3644473d91f065bf35225aec9e0cfd in (buy_token_address, sell_token_address) -- exclude 404
    or 0x9e9fbde7c7a83c43913bddc8779158f1368f0413 in (buy_token_address, sell_token_address) -- exclude PANDORA
    or 0x6c061d18d2b5bbfbe8a8d1eeb9ee27efd544cc5d in (buy_token_address, sell_token_address) -- exclude MNRCH
    or 0xbe33f57f41a20b2f00dec91dcc1169597f36221f in (buy_token_address, sell_token_address) -- exclude Rug
    or 0x938403c5427113c67b1604d3b407d995223c2b78 in (buy_token_address, sell_token_address) -- exclude OOZ
    or 0x54832d8724f8581e7cc0914b3a4e70adc0d94872 in (buy_token_address, sell_token_address) -- exclude DN404
    -- Temporary exceptions for Feb 13..Feb20, 2024 are starting here
    or 0xb5c457ddb4ce3312a6c5a2b056a1652bd542a208 in (buy_token_address, sell_token_address) -- exclude EtherRock404
    or 0xd555498a524612c67f286df0e0a9a64a73a7cdc7 in (buy_token_address, sell_token_address) -- exclude DeFrogs
    or 0x73576a927cd93a578a9dfd61c75671d97c779da7 in (buy_token_address, sell_token_address) -- exclude Forge
    or 0x3f73eaeba8f2b2699d6cc7581678ba631de5f183 in (buy_token_address, sell_token_address) -- exclude DEV404
    or 0x7c6314ccd4e34346ba9c9bd9900faafb4e3711b0 in (buy_token_address, sell_token_address) -- exclude ERC404X
    or 0xe2f95ee8b72ffed59bc4d2f35b1d19b909a6e6b3 in (buy_token_address, sell_token_address) -- exclude EGGX
    or 0xd5c02bb3e40494d4674778306da43a56138a383e in (buy_token_address, sell_token_address) -- exclude OMNI404
    or 0x92715b8f93729c0b014213f769ef493baecedacc in (buy_token_address, sell_token_address) -- exclude WIFU 404 
    or 0x413530a7beb9ff6c44e9e6c9001c93b785420c32 in (buy_token_address, sell_token_address) -- exclude PFPAsia. 
    or 0xe7468080c033ce50dd09a22ad1e58d1bda69e436 in (buy_token_address, sell_token_address) -- exclude YUMYUM. 
    or tx_hash = 0x41418cef26e608ed47a5c4997833caaa2366a0163173286140da28a32e37b25d -- temporary solution
    or tx_hash = 0xdf415f3048d401c9ca7bf079722be96aaed3d2d2b5c0e12b7dc75d6eec30b3d4 -- temporary solution
    or tx_hash = 0x15b9906aa2039ccbc9ae9fab0f0c7517e9c88c41b74cd8a09f202803d37f6341 -- temporary solution
    or tx_hash = 0x3a71df0f6898b229c3643d4703b56d7510d455c65649cb364e5b69cadf5d1d37 -- temporary solution
    or tx_hash = 0xc9bcb4c8c68d4edcb97403131d28416a418ae537c43e9feca50f11ca744c079e -- temporary solution
    -- for week of Feb 13, 2024 until Feb 20, 2024
    or tx_hash = 0x60157b1891dbdbcdc88c637079c4c9e37d5fe943bf3ffff14412b33bf7125ad1
    or tx_hash = 0x414e72fa7c061a1b2c5905f21e41d8cb5500ec9043b65b317cd20362f4eff757
    or tx_hash = 0x829d0583b647581cdd8f01f62e6715a7c6333b499f164995601217bde1976a09 -- internalization involving PANDORA
    -- for week of Feb 27, 2024 until March 5, 2024
    or tx_hash = 0xc27463c9c6084a1067488d75732d37d85bf34f5d7882222eceba2d7f83c85dfe
    or tx_hash = 0x65ae7ff7419777bb0e81ebfffc55da654c89834f23065f7e62aac5891a8e0abb
    or tx_hash = 0xf82ad7fc31169a51e24f30677cf6ba5c337fd3c501551635cd94b768c9a1d5b1
    or tx_hash = 0xe3cf17ab69ff9efd72d44cd855db35c76732895a5413d13851871e2f6cbe701c
    or tx_hash = 0xdcddf86f9439522158c259ad5eabc6574b4f176631b09a27f450f2c6cc420993
    or tx_hash = 0xc3e8f21faea641132927eb9e7c5f5321cb90ef50300bce896a4277ee9a2bbd1a
    or tx_hash = 0x5ae92778c6d18d8391ecf87a10e4de381e558bab0eb939a69d596801d47e8b97
    -- for week of March 5, 2024 until March 12, 2024
    or tx_hash = 0x245cad3a40ab34ae6a6e79e050ec6946d80b1a501b345412bd33a8e0df6a1ea6
    or tx_hash = 0xc2e44a4abcfb719b3038ac346b89b2fd1391abcc3d9938954a6bc81495143619
    or tx_hash = 0xc7928590347245ccaa1b4794cc348c0d72b757f4376c023a8f67238c81280046
    or tx_hash = 0x3fbcf3aa9024d82e23c262a1e3c8ecbca83279e92fd0e2e147f3125762ca1bb4
    or tx_hash = 0x98d74de9f96d14f38cdabda0446d61e7a49db68f71670f1e66830bf40a0ca003
    or tx_hash = 0xaf56355b74863b9129824fbf31a182bc54bd94501b6c557778f060a2c2cd9973
    or tx_hash = 0x44daaccec0c6718b557258348b9f8736fbb94ea97d7d21336c8e0da3e309f84f
    or tx_hash = 0x5ae0810874dd9f4657fced144563af501e0d43265ac1b7a2ec2c7b0291b84af8
    or tx_hash = 0x5cb533bd94ac4c3703b6fe7e52c2089ffe9dc6675b2f6b96242a8b340cbd1430
    or tx_hash = 0xcde2671c02065773ef8efee2e39acb8a78bea213fffaf692f454e7bd97711c70
    or tx_hash = 0xe1d3480b3c99b2387cf6b0724abcd936fd7f888d9a5ee70be96cdcfc94eec29b
    or tx_hash = 0x0b8f6950f3f54034530bed401331172849b7e16bf964e3b92d838451a0a29d64
    or tx_hash = 0x1879438c38fae1ed205063218086cca0a600fe0263ab0f6cfad0f31bd3355b15
    or tx_hash = 0xbcfb968a772f0b68a7ecf1858925a2876e86a8d6941e7c53d0bf666aa66e02c0
    or tx_hash = 0xc06297f9a24faae8b165de7abf793dd53dfe8abd5be67ceb0964145a74e244cd
    or tx_hash = 0xb6e93c1b30b2020c943dd956ba5c797cecfbbdf121b0ee7ea1c0552b22e031a4
    or tx_hash = 0xcd9fb72ab8a61b6706e8ecb79939111318e770fcd8c29e8d49826c57b48cdb6b
    or tx_hash = 0x2a9a9b9c837bf8cd89dabf91825ca678fd2e2d506a0686cf3774e6c77748d319
    or tx_hash = 0xfb239a1959d1db58be3f33ab13c1aa45ea39e7b97653590b0f320b4e446ae6ab
    or tx_hash = 0x7b2588fda96cb480d6a055f327c368b3e1a5638f489e1b1ede9d143361b94c65
    or tx_hash = 0x00198fdad1047b31299d8d91afa71467cc491d643e730f9ef5bb9a9e7a5cfdad
    -- for week of March 12, 2024 until March 19, 2024
    or 0x382e57ca8e4c4db9649884ca77b0a355692d14ac in (buy_token_address, sell_token_address) -- exclude XYXYX
    or tx_hash = 0x46c5064ffae9d4f0132fcaf9e75b169aecd23b0834b7743bc5280770ace3a10e
    or tx_hash = 0xba20e80f1e055865e594f868843f5f642b896291c91afa39cde1820e3129f543
    or tx_hash = 0xeb18483d07998f33952bba494aff9542283afcd2867b12fcfdc82672f87a97a3
    -- for week of March 19, 2024 until March 26, 2024
    or tx_hash = 0xe926b6c67228cc5ff3f44a4ea46104468d984a68316871b0b5165065f8c0feca
    or tx_hash = 0xf1495b9c437e50454ff525902b700aec7a1b8d75d47798333480f6a445082064
    or tx_hash = 0x8901360b463e470f44f91511b89f97197d7d5d7ca48a39ad51098f1fe630727b
    or tx_hash = 0xf04d5c32a1d1fa791a974270f7466383f2e2677fa667942bc862bfae4e84e502
    -- for week of March 26, 2024 until April 1, 2024
    or tx_hash = 0xa93c3d40f94c9feb75f6e0453d7e338666ce842b675017a01e7bd20d415dbaa1
    or tx_hash = 0x03e7a74da8a2f8318caa38da3ab83e5540e7324340552d04e9e0c4f97d763dc4
    or tx_hash = 0x844195cb1e04cc532b9595e03b86ca6481bd36e38b974d72f6ef11caf21a6875
    or tx_hash = 0x0b419b76bb53b9903bdfbcf47da50491e5a95833b0a4ac08c2cd1a12afa99cf3
    or tx_hash = 0x1a1212026760ad3d01ec678f87c6dfe3ac47ce9bcb1f7b09675462664b23f921
    or tx_hash = 0x033cf6c93b222900b6b7f41d1833efc19d5c1b3e7701e4335979716f60f7ade2
    or tx_hash = 0x862f950718b7e14225625277f6d16dfb9d48c923b936691f5149863b498535f2
    or tx_hash = 0x54ae0ccd530438de78fe7317a76abe7fffeb9bb15673b6d0885839eb0aa8184b
    or tx_hash = 0x9233c85da85670a0ae0fa61404387d1fbd88319aec4fafe13eee9605cc521462
    -- for week of April 2, 2024 until April 9, 2024
    or tx_hash = 0x924a9d66594ebd8e85204ce1a9ed853d4151923519d146bf6bcbf61bd5978837
    or tx_hash = 0x491faa01f97bf667a9a014bf8ea0200db42eaaf3968746f9e7005a75d10ad624
    or tx_hash = 0x907b052adeb8ffca4948908b38628ef6af425f630dad1c054f96e577ca4ffb7b
    or tx_hash = 0x70b8a208ac86b1290b8bb300dd0d2faef93fb91327da40b8bd4f55275ee4b4ab
    -- for week of April 9, 2024 until April 16, 2024
    or tx_hash = 0xd00adcd9c7ae9612a10afb390fbe81409b7972c10ac582930afe7e1c23298d62
    or tx_hash = 0xe8012a6474bd9db791f4157c487a8880c06e41275272113b5fad03c04ee2cba7
    -- for week of April 16, 2024 until April 23, 2024
    or tx_hash = 0x142adfe0b863a6621579f501859733de243d4b8d673b9d50150c8e99ec7387eb
    -- for week of April 30, 2024 until May 7, 2024
    or 0x730bcbe5cdc1a3061dfe700774b7b8dd1d4173db in (buy_token_address, sell_token_address) -- exclude DaVinci ERC-721
    -- for week of May 7, 2024 until May 14, 2024
    or tx_hash = 0x360803f2df15d66fef4afdeb981798c988d72078c400acdb20e10d5018cb1f46
    or tx_hash = 0xca73fed6c8e0a7b44685d74884025d25bee4c1fba836e7f211331e2b2bfcdc09
    or tx_hash = 0x4c723a4e944425e85fb34ebf9943c5ddadce0b4d388259181e11134aa8fdefa3
    or tx_hash = 0xc5cc04cb47695b1a7e32341bf254054698097596420d7822760e96548a739a16
    -- for week of May 21, 2024 until May 28, 2024
    or tx_hash = 0x0531bcb0e1c6e9c743b787c6dbf8b6f9c5ff67682408f74c11a567b74f31fedc
    or tx_hash = 0x03943dd6ea4bb2e1b4ff03eae70d19efb6921ecd0666c213ff20a84c31a74de5
    or tx_hash = 0xd2e723895af68036f2623d016a6d4ed7dd4cdef919220c847293b7fb49be5ed6
    or tx_hash = 0xec44a4a88420de45a02147a20dd4a25c49ac53c3c06bb840ff0b093f7db88cc1
    or tx_hash = 0x66bbee193158b5c172cc03486e91a4de086a8375e2ab421ef0d8e23a7de13dd2
    or tx_hash = 0x8af8e15f5f425ce29e05743e1586f6e39f138ac16836a2ee888fe8cf181493ad
    or tx_hash = 0xb07d4ed63a4cb373d0676f7fc59d4fc0785c1a025f5f4950e5909ee44c546343
    or tx_hash = 0x73c118eedaab117c8ff88262132e75e0b9969a0ed9c789bc23cdad17f11fa159
    or tx_hash = 0x129c49166827f038101d6a5735e1c85269a1430300136471817b60995dc97de1
    or tx_hash = 0x2746c84a8c9e08d72c362ecacfc471473911b953f62f2a45899e994372a38f5c
    or tx_hash = 0x3c7ec9f66b75b8e2e0d8174adf761b2f483abb0537919dcc46acbc693bda2982
    -- for week of May 28, 2024 until June 4, 2024
    or tx_hash = 0xc93f75665df1d4f62ee2447c2ffa40a628b5904878450a05e27de597bdb1470e
    or tx_hash = 0x68a10171dad3d5ee4b0304926d33501cf03c77d308d52bfd43e5d0d9cd021d89
    or tx_hash = 0x4930273761cdd2fba156adc4d3556e3b7d6655089a1bb22b0b08f94edd0b21c7
    or tx_hash = 0x23638eb4c4ee00ddf855fc8daf91e9baf7c803e747dbfa26e36f58eb4576df8e
    -- for week of June 11, 2024 until June 18, 2024
    or tx_hash = 0xbe34ce42dae8faae31876616d8a3359fe1f22b598a753a518065dd7e73d2b1e2
    or tx_hash = 0x6098baad61108c4db73d4fca97f0a3c97f156524f71b8d65f9fa4e4f208ae664
    or tx_hash = 0x9d93a23556cfaf7d63155ef4b317a9168bcef3e4a5642320bdc013ece70fe909
    or tx_hash = 0xddcec62f27d370edf84d4ae6787e401eaea62a47257d9c3c4295e7ac6b792dce
    or tx_hash = 0xb904b88ef329cceba0ff90481dadf8fd17dc5371516808a4ae4de128eb14821c
    or tx_hash = 0x883efbacc1a9331b4a15cac3f7f28ec0c9e187658bf27108f656e8275915a87a
    or tx_hash = 0x357f85c50d1b8c484d4181ce300b2ab64db33832c4a765852444503f5a0f906b
    or tx_hash = 0xd24b3c8aaefaa9e4f467ca7fa8156afb18f9218e0a34de45f2f524f2bb4879ef
    or tx_hash = 0x7fe800c0e51970d25a3ee0e2d899c0c8fc128fdd6f5f07e11f8e7113cd6d11a5
    or tx_hash = 0xd03209f71ddd2502f73627ccb81ed54d8a2a7cd42a0920cec53a511f2d757c4b
    or tx_hash = 0xf19e56beb9f2a4ff548c4f5f3d8f3a368a8fab9d65d7aa1a07f6278f0c6daa4a
    -- for week of June 18, 2024 until June 25, 2024
    or tx_hash = 0x9e53bd81a1ea279403c41dc3c92ac57b70bfd75ee8fa2d30e1029050f1bc5730
    -- for week of July 2, 2024 until July 9, 2024
    or tx_hash = 0xaf40d801d03975135c539707b1ecc998730750c0f9b185a0cd144e0d461f53cd
    or tx_hash = 0x7e65980f09bb8ec67f6dddd82e6a2f2a990501c3c6928b423903e2642dc5d73d
    -- for week of July 9, 2024 until July 16, 2024
    or tx_hash = 0x1d9577d7c8e856a74f664b4d1be3d7488e33b730f8c190e433278d8e3cefc2cb
    or tx_hash = 0x69f9ab97c687a68575ced1d57ff71e69b680746b6473ac269d3ec3a21f686b33
    or tx_hash = 0x89f921cf4d93eeba622b4b333e92c023698d32263fe29fdb93fac34656f86c06
    or tx_hash = 0xb4f58c9e7132e49fe10ea3e0ec90c37a68f11fd0454f750387ff9dbc633a811e
    or tx_hash = 0xc78513b3146004bc7d3e0765c92e13c4b91adb9a16002aea2601ec4a68d1eab5
    or tx_hash = 0xd6ab356a216afca8a40e9343f4cdffe64ec5999a47a10fe7ce4427aba7b6cf96
    or tx_hash = 0x87030eb545efebe7f59718608eee475eea95f5ba77bb1c1f46d2a46ded245b13
    or tx_hash = 0x9e216f57d34e31ac61aa1316c3b9a5e460ae1eb7d6b8bf017fdd4af083566219
    -- for week of July 16, 2024 until July 23, 2024
    or tx_hash = 0x0cd1c6d5c0d01b114ab85a92cf52eabd41bc7eb48692069f8a60ca24ef284e1d
    or tx_hash = 0xfc8af62481cf21f6c91d1e64afb4079d95a3c76f3d3ad6626206ca6c5e9c6126
    or tx_hash = 0x18b436c1f14b491282c453d7bc67295f95108d055001719d6ffd7110fe6513cd
    or tx_hash = 0x27c9fd96c1bbcf75c521ac774f270f9502bd4ce4816e4f2c9d75e64b90fb9778
    or tx_hash = 0x9a0d7ac0eb92197e4122c56332f3bf6ee6d6ce6cd6a03f988db268e4f88a3c5d
    -- for week of July 23, 2024 until July 30, 2024
    or tx_hash = 0x6f4638194282021a3ae2a80c778ff7829d4b269af6ac1cd612744a9d3dbd86fb
    -- for week of August 06, 2024 until August 13, 2024
    or tx_hash = 0xbd8cf4a21ad811cc3b9e49cff5e95563c3c2651b0ea41e0f8a7987818205c984
    -- for week of September 24, 2024 until October 1, 2024
    or tx_hash = 0x0ee0a609c54cb006d024a4d009db8751730c064b26524379793144c07c3575b3  -- one-off miscalculation of network fee for a jit order, thus it erroneously reported negative slippage
    or tx_hash = 0x7087eb55854228a30c864a9ee4d6c4072d37d53bf4d0404f1064c5b33b7aa96d
    or tx_hash = 0x84eb7aef07139e9558f08ac92b857b727f64c0f44d92a572078f45b7d77ebe74
    -- for week of November 5, 2024 until November 12, 2024 -- UPDATE: commented out tokens so as not to affect follow-up weeks
    or tx_hash = 0x7560b13901877f7d7ee5ddff01d41a5ea5ad2d36460a5c7f4d149db4d0b2bfe9
    or tx_hash = 0x1ba38b8aad030febd89c8d2297f198f893c26097e13a988ef77afe9a8d8f1e9a
    --or 0xf19308f923582a6f7c465e5ce7a9dc1bec6665b1 in (buy_token_address, sell_token_address)
    --or 0x96a5399d07896f757bd4c6ef56461f58db951862 in (buy_token_address, sell_token_address)
    --or 0xd7fa4cfc22ea07dfced53033fbe59d8b62b8ee9e in (buy_token_address, sell_token_address)
    --or 0x66b5228cfd34d9f4d9f03188d67816286c7c0b74 in (buy_token_address, sell_token_address)
    --or 0xCC42b2B6D90e3747c2b8E62581183A88E3Ca093a in (buy_token_address, sell_token_address)
    --or 0x2614f29c39de46468a921fd0b41fdd99a01f2edf in (buy_token_address, sell_token_address)

    -- for week of Nov 26 - Dec 3, 2024
    or tx_hash = 0x7a56ca3e7f2a6a41f829fed953c321010a3ec3c655909f382bff676d39e18dd0
    or tx_hash = 0x46c5c6768774b92af828fb18be81e88baa28d28292f7c2d048d8c3080ea9da5c -- involving token that basically couldn't trade afterwards
    or tx_hash = 0x4b2abb45491995674442a041a82e94a863428bca4f4c956805f8ec9f802c9fba

    -- for week of Dec 17 - Dec 24, 2024
    or tx_hash = 0xf7a16b761cb5b09f68350607675af07ce48dc772d4ff1c6e9eda5e4e3f62a139
    or tx_hash = 0xd5295f40e2685c5fd4de432c2d377f9d5e71877c187fae5d0fc44a5ed66fc142 -- involving token that basically couldn't trade afterwards
    or tx_hash = 0x6c43b1a12293d014cec6f77078d7947a85afd849f159356e56e8fc336cdb6a7c

    -- for week of Dec 31, 2024 - Jan 7, 2025
    or tx_hash = 0xd1ca15a5921781979d4a156da233668940d580cf5e04a721f874aeae7c4748e4

    -- for week of Jan 7 - Jan 14, 2025
    or tx_hash = 0x132bdc1984ea83665dd590fa5c98c82752426ea060e32ca2151e38643c6efbd6
    or tx_hash = 0xbbffb1584a20bdff8abfabfd66c773ee3bef16e7bf9549ffdfa88781bae347eb

    -- for week of Jan 14 - Jan 21, 2025 on mainnet
    or tx_hash = 0x8254ae2fc56214657139b0a1e934d00937019bd8b6f873641a34f1edbe0fbc91
    or tx_hash = 0xa8c8f763a7039fd76ada9067c5868a2a249c1b00c1b30533dff73b08ddf309d7
    or tx_hash = 0xbd232581323f79d6a5e31d3f42209b3769e1cba010469948fb7d3d3f0e5c8060
    or tx_hash = 0xa9813d70a87c609f76111fc74c52d820d2fff2ba68f179c0a339387034555ef8

    -- for week of Jan 21 - Jan 28, 2025 on mainnet
    or tx_hash = 0xb99ea4b15a54ee68d02d695c5cdd1f60cb33d2f2d50da5bca277c8adae108208
    or tx_hash = 0x74e730407d3061e60527629123c1e7b43e3e73b4ba4232bd077c7109ac5e8325
    or tx_hash = 0x08afb56d779bc2034bfcc78a1d470cc82c8c55c4394fda3875d426de869f8ca2

    -- for week of Feb 4 - Feb 11, 2025 on mainnet
    or tx_hash = 0xf5a574a1bac9d5ffabeb9c959af3018a41b1570cbf4a0ddf84e9ddda83dac6df
    or tx_hash = 0x069b2c966d9c95119983ea51995d642f320ae3b8252ee9af643afeb6d7ca3d69
    or tx_hash = 0x588397a4c1d4671e65a3d8355f3e7c9ce2dc6d6013e06bb3332c3357ee284ba8
    or tx_hash = 0xca7672ef67787948743f5d2c6c56db8946488bfefd0566f9f91a7f91f66ef2a3
    or tx_hash = 0x1082ad668f906ac04231296c427bf57e6589e3a907f3f876752413fc3b98420d

    -- for week of Feb 11 - Feb 18, 2025 on mainnet
    or tx_hash = 0x33955f26dcfa23cf4c20c0eca1c914c140c01397c0e521adf494b71f871b9f7f
    or tx_hash = 0x50d5f477b7f62bf2f4cb258e9d5ac375033df56f4c0cc07de4e9c585d90aa390

    -- for week of Feb 25 - March 4, 2025 on mainnet
    or tx_hash = 0x34ad176862c6d51bdd8232b67bf4c689cf0707cec18e17e9731017a1f3823f6a

    -- for week of March 4 - March 11, 2025 on mainnet
    or tx_hash = 0x1f14b47cca77fba18f6ad87372c9f942617faf9c787622bbd8f56d187ef069fa

    -- for week of April 15 - April 22, 2025 on mainnet
    or tx_hash = 0x91f810f43903c11b99ccb6b4deeda464261b655b0bd546ef4edbcfdd6bad5ddd

    -- for week of April 29 - May 6, 2025 on mainnet
    or tx_hash = 0x683fff5607fac2fd249050794fedd47875e3f94762ee885c7514b080787989c5

    -- for week of May 20 - May 27, 2025 on Mainnet
    or tx_hash = 0xac8de01cd4f8737c95bf66d451ce8d2eda31802a41c9629e4c4557e943e13edc
    or tx_hash = 0xf886919e66f466b7381c4c939e131038a745990357ae9d77ad073668ebb08238

    -- for week of June 10 - June 17, 2025 on mainnet
    or tx_hash = 0x0183756d30137630a9c1f7c02c9ec904751147e0fedbd7e529f31d05aac04baf
    -- due to bug with slippage accounting for flashloans txs

    -- for week of June 17 - June 2024, 2025 on mainnet
    or tx_hash = 0x939879ff06f5c9e94d3e27f3b78cbcbb8eae72782a5fdcac831c743e2a5492e1

    -- for the week of June 24 - June 30, 2025 on mainnet
    or tx_hash = 0xc60e65f001e2cede1a2399fbf40e049b7f7bd57d8a982de66ef4e44c23967589

    -- for the week of July 1 - July 8, 2025 on mainnet
    or tx_hash = 0x46cbd1134448f5721cb6d28372db0ff4676e5ec1855b843abf6059cf7b244be9
    or tx_hash = 0xa91fa23e7f971aa883e3ce52c6b555c6cc687a736cfa2d3f3ad5ea9088c373aa

    -- for the week of July 8 - July 15, 2025 on mainnet
    or tx_hash = 0xdc12efa6448aba96ba49d444c1f9d950a846421771007b2089332b7b5580eb22
    or tx_hash = 0xc045588e98e1f3b7a8dc333327a62c52a6b251ae9d6b27e7abd07edf3a34d0be
    or tx_hash = 0x235226999da35cef46825156eb6b27eec3101a759c867907f931528b8b6f1ebd
    or tx_hash = 0x9305cb182849223f18422f1b469fa14cade009f8427b2f409a16ef6b968ad8a0

    -- for the week of July 15 - July 22, 2025 on mainnet
    or tx_hash = 0x71aea9d790fae3539cd45d0a01d3b8651c5a4c5ceaaadf1e224f7a2c86f2794a
    or tx_hash = 0x2f7b5c5942103f781c57022816b74f19069cf8546f26373e6cc227972da75d48

    -- for the week of August 26 - Sept 2, 2025 on mainnet
    or tx_hash = 0x5c550c99a39a7315a699d6e5c6592038f26f7d93b787cf63312e38a72df17e2a
    or tx_hash = 0xbd68462f2d22340ed4e2818b0fac68f9ab78afe07e9ed6e73f72cbde0c40735f

    -- for the week of Sep 2 - Sep 9, 2025 on mainnet
    -- contains some weird transfer from the vault relayer and the token is not there anymore while no transfers can be detected
    or tx_hash = 0x8625c78713352b9bc1c5583d1f0fa25e0122bd7a2be0800f5d5af6fcf626d461
    -- price of imbalance is near zero now
    or tx_hash = 0xa9069cad6ecc62006f8abfacb7fb0919466682e054e59d14ae9876ca34fbaba1
    -- wrong LUNA price
    or tx_hash = 0x05515bfee30868311615c251eff71c1246dacca23fde35f665369d0ae50e03c1

-- Base
union all
select distinct tx_hash
from
    cow_protocol_base.trades
where
    -- for week of Jan 14 - Jan 21, 2025 on Base
    tx_hash = 0x64eddc682e60f965378af9ebfa4095b07cfababd2e16289eb7a309c7c4e57969
    or tx_hash = 0x6e2966f68e44533e91afe81311e0cd6fe0866dc8c9328b7bb2fe9b3f6813a6d7
    or tx_hash = 0x7f50f5cb2893d688d695c391dafc60b6a22eb76670e6c699104d979a1ffc2b20
    or tx_hash = 0xadea382edef5e7b99c80a27b8b67abea8ba513ad15ba7681d0c5a7d1ff01b0df
    or tx_hash = 0xdca910d167b67fd94df3328e6dd065e1fc6de277013af4826b8c35eb5f5a13b4

    -- for week of Feb 11 - Feb 18, 2025 on Base
    or tx_hash = 0xff82de50012062ce5672c086ccaf2b6b5b13b8440736720b1bba83454034c697

    -- for week of March 18 - March 25, 2025 on Base
    or 0x22af33fe49fd1fa80c7149773dde5890d3c76f3b in (buy_token_address) -- exclude BNKR

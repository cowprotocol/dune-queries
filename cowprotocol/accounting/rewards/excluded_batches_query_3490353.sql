-- Exclude the following txs/trades from slippage accounting due to wrong Dune prices
select distinct tx_hash
from
    cow_protocol_ethereum.trades
where
    0xf5d669627376ebd411e34b98f19c868c8aba5ada in (buy_token_address, sell_token_address) -- exclude AXS (Old)
    -- mixed ERC20/ERC721 tokens:
    or 0xf66434c34f3644473d91f065bF35225aec9e0Cfd in (buy_token_address, sell_token_address) -- exclude 404
    or 0x9E9FbDE7C7a83c43913BddC8779158F1368F0413 in (buy_token_address, sell_token_address) -- exclude PANDORA
    or 0x6C061D18D2b5bbfBe8a8D1EEB9ee27eFD544cC5D in (buy_token_address, sell_token_address) -- exclude MNRCH
    or 0xbE33F57f41a20b2f00DEc91DcC1169597f36221F in (buy_token_address, sell_token_address) -- exclude Rug
    or 0x938403C5427113C67b1604d3B407D995223C2B78 in (buy_token_address, sell_token_address) -- exclude OOZ
    or 0x54832d8724f8581e7Cc0914b3A4e70aDC0D94872 in (buy_token_address, sell_token_address) -- exclude DN404
    -- Temporary exceptions for Feb 13..Feb20, 2024 are starting here
    or 0xB5C457dDB4cE3312a6C5a2b056a1652bd542a208 in (buy_token_address, sell_token_address) -- exclude EtherRock404
    or 0xd555498a524612c67f286dF0e0a9a64a73a7Cdc7 in (buy_token_address, sell_token_address) -- exclude DeFrogs
    or 0x73576A927Cd93a578a9dFD61c75671D97c779da7 in (buy_token_address, sell_token_address) -- exclude Forge
    or 0x3F73EAEBA8f2b2699D6cC7581678bA631de5F183 in (buy_token_address, sell_token_address) -- exclude DEV404
    or 0x7c6314cCd4e34346Ba9C9bd9900FaafB4E3711B0 in (buy_token_address, sell_token_address) -- exclude ERC404X
    or 0xe2f95ee8B72fFed59bC4D2F35b1d19b909A6e6b3 in (buy_token_address, sell_token_address) -- exclude EGGX
    or 0xd5C02bB3e40494D4674778306Da43a56138A383E in (buy_token_address, sell_token_address) -- exclude OMNI404
    or 0x92715b8F93729c0B014213f769EF493baecEDACC in (buy_token_address, sell_token_address) -- exclude WIFU 404 
    or 0x413530a7beB9Ff6C44e9e6C9001C93B785420C32 in (buy_token_address, sell_token_address) -- exclude PFPAsia. 
    or 0xe7468080c033cE50Dd09A22ad1E58D1BDA69E436 in (buy_token_address, sell_token_address) -- exclude YUMYUM. 
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
    or 0x382E57cA8e4c4DB9649884ca77B0a355692D14AC in (buy_token_address, sell_token_address) -- exclude XYXYX
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
    or 0x730BCBe5Cdc1a3061Dfe700774b7B8dd1d4173DB in (buy_token_address, sell_token_address) -- exclude DaVinci ERC-721
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
    or tx_hash = 0xF7A16B761CB5B09F68350607675AF07CE48DC772D4FF1C6E9EDA5E4E3F62A139
    or tx_hash = 0xD5295F40E2685C5FD4DE432C2D377F9D5E71877C187FAE5D0FC44A5ED66FC142 -- involving token that basically couldn't trade afterwards
    or tx_hash = 0x6C43B1A12293D014CEC6F77078D7947A85AFD849F159356E56E8FC336CDB6A7C

    -- for week of Dec 31, 2024 - Jan 7, 2025
    or tx_hash = 0xd1ca15a5921781979d4a156da233668940d580cf5e04a721f874aeae7c4748e4

    -- for week of Jan 7 - Jan 14, 2025
    or tx_hash = 0x132BDC1984EA83665DD590FA5C98C82752426EA060E32CA2151E38643C6EFBD6
    or tx_hash = 0xBBFFB1584A20BDFF8ABFABFD66C773EE3BEF16E7BF9549FFDFA88781BAE347EB

    -- for week of Jan 14 - Jan 21, 2025 on mainnet
    or tx_hash = 0x8254AE2FC56214657139B0A1E934D00937019BD8B6F873641A34F1EDBE0FBC91
    or tx_hash = 0xA8C8F763A7039FD76ADA9067C5868A2A249C1B00C1B30533DFF73B08DDF309D7
    or tx_hash = 0xBD232581323F79D6A5E31D3F42209B3769E1CBA010469948FB7D3D3F0E5C8060
    or tx_hash = 0xA9813D70A87C609F76111FC74C52D820D2FFF2BA68F179C0A339387034555EF8

    -- for week of Jan 21 - Jan 28, 2025 on mainnet
    or tx_hash = 0xB99EA4B15A54EE68D02D695C5CDD1F60CB33D2F2D50DA5BCA277C8ADAE108208
    or tx_hash = 0x74e730407d3061e60527629123c1e7b43e3e73b4ba4232bd077c7109ac5e8325
    or tx_hash = 0x08afb56d779bc2034bfcc78a1d470cc82c8c55c4394fda3875d426de869f8ca2

    -- for week of Feb 4 - Feb 11, 2025 on mainnet
    or tx_hash = 0xf5a574a1bac9d5ffabeb9c959af3018a41b1570cbf4a0ddf84e9ddda83dac6df
    or tx_hash = 0x069B2C966D9C95119983EA51995D642F320AE3B8252EE9AF643AFEB6D7CA3D69
    or tx_hash = 0x588397A4C1D4671E65A3D8355F3E7C9CE2DC6D6013E06BB3332C3357EE284BA8
    or tx_hash = 0xCA7672EF67787948743F5D2C6C56DB8946488BFEFD0566F9F91A7F91F66EF2A3
    or tx_hash = 0x1082AD668F906AC04231296C427BF57E6589E3A907F3F876752413FC3B98420D

    -- for week of Feb 11 - Feb 18, 2025 on mainnet
    or tx_hash = 0x33955F26DCFA23CF4C20C0ECA1C914C140C01397C0E521ADF494B71F871B9F7F
    or tx_hash = 0x50D5F477B7F62BF2F4CB258E9D5AC375033DF56F4C0CC07DE4E9C585D90AA390

    -- for week of Feb 25 - March 4, 2025 on mainnet
    or tx_hash = 0x34AD176862C6D51BDD8232B67BF4C689CF0707CEC18E17E9731017A1F3823F6A

    -- for week of March 4 - March 11, 2025 on mainnet
    or tx_hash = 0x1F14B47CCA77FBA18F6AD87372C9F942617FAF9C787622BBD8F56D187EF069FA

    -- for week of April 15 - April 22, 2025 on mainnet
    or tx_hash = 0x91f810f43903c11b99ccb6b4deeda464261b655b0bd546ef4edbcfdd6bad5ddd

    -- for week of April 29 - May 6, 2025 on mainnet
    or tx_hash = 0x683fff5607fac2fd249050794fedd47875e3f94762ee885c7514b080787989c5

-- Base
union all
select distinct tx_hash
from
    cow_protocol_base.trades
where
    -- for week of Jan 14 - Jan 21, 2025 on Base
    tx_hash = 0x64eddc682e60f965378af9ebfa4095b07cfababd2e16289eb7a309c7c4e57969
    or tx_hash = 0x6E2966F68E44533E91AFE81311E0CD6FE0866DC8C9328B7BB2FE9B3F6813A6D7
    or tx_hash = 0x7F50F5CB2893D688D695C391DAFC60B6A22EB76670E6C699104D979A1FFC2B20
    or tx_hash = 0xADEA382EDEF5E7B99C80A27B8B67ABEA8BA513AD15BA7681D0C5A7D1FF01B0DF
    or tx_hash = 0xDCA910D167B67FD94DF3328E6DD065E1FC6DE277013AF4826B8C35EB5F5A13B4

    -- for week of Feb 11 - Feb 18, 2025 on Base
    or tx_hash = 0xFF82DE50012062CE5672C086CCAF2B6B5B13B8440736720B1BBA83454034C697

    -- for week of March 18 - March 25, 2025 on Base
    or 0x22af33fe49fd1fa80c7149773dde5890d3c76f3b in (buy_token_address) -- exclude BNKR

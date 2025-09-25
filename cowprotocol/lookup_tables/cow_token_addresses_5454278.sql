SELECT *
FROM (VALUES
    ('ethereum', 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab),
    ('gnosis',   0x177127622c4a00f3d409b75571e12cb3c8973d3c),
    ('arbitrum', 0xcb8b5cd20bdcaea9a010ac1f8d835824f5c87a04),
    ('base',     0xc694a91e6b071bf030a18bd3053a7fe09b6dae69),
    ('polygon',  0x2f4efd3aa42e15a1ec6114547151b63ee5d39958)
) AS t(blockchain, address);

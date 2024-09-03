-- Exposes all relevant information about MEV Blocker connected builders
-- Builders that disconnected and then re-connected again have separate 
-- entries for each subscription period.
--
-- To end a subscription, change the end_block of the exiting builder to 
-- the last block on which they received flow.

SELECT
    billing_address,
    label,
    bond_provider,
    start_block,
    end_block,
    builder_addresses,
    extra_data
FROM (
    VALUES
    (0x816E4a1589e363720c15c54dFD2eFd16f6377070, 'beaverbuild', null, 19557289, 999999999, array[0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5], array[0x6265617665726275696c642e6f7267]),
    (0xcf7c69c7aCF62179d29FA3f47de62226B58Ad992, 'Titan', null, 19557289, 999999999, array[0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97], array[0x546974616e2028746974616e6275696c6465722e78797a29]),
    (0xFd39bc23d356a762cF80F60b7BC8D2A4b9BCFE67, 'Flashbots', 0xa489faf6e337d997b8a23e2b6f3a8880b1b61e19, 19714018, 999999999, array[0xdf99A0839818B3f120EBAC9B73f82B617Dc6A555, 0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5, 0x389c8703E9c61F05fE17803eb648653fD3f2aB1C], array[0x496c6c756d696e61746520446d6f63726174697a6520447374726962757465]), --noqa
    (0xb100d6f4fc91c8c666f615e3efe4c4c90584a610, 'penguinbuild', null, 19760197, 19790340, array[0xf15689636571dba322b48E9EC9bA6cFB3DF818e1, 0x1F1522b9621975321C3578BD40db5f07a122FC0D], array[0x70656e6775696e6275696c642e6f7267,0x4070656e6775696e6275696c642e6f7267]), --noqa
    (0xb100d6f4fc91c8c666f615e3efe4c4c90584a610, 'penguinbuild', null, 19860899, 20142250, array[0xf15689636571dba322b48E9EC9bA6cFB3DF818e1, 0x1F1522b9621975321C3578BD40db5f07a122FC0D], array[0x70656e6775696e6275696c642e6f7267,0x4070656e6775696e6275696c642e6f7267]), --noqa
    (0xB02BDb9CA42122F44ee72b7a55acaad7938c6b8d, 'rsync-builder', null, 20061091, 999999999, array[0x1f9090aae28b8a3dceadf281b0f12828e676c326], array[0x407273796e636275696c646572,0x7273796e632d6275696c6465722e78797a]) --noqa
)
    AS t (billing_address, label, bond_provider, start_block, end_block, builder_addresses, extra_data) -- noqa: AL05
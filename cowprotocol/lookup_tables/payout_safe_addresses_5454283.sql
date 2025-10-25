select *
from (
    values
    ('ethereum', 0xa03be496e67ec29bc62f01a428683d7f9c204930),
    ('gnosis', 0xa03be496e67ec29bc62f01a428683d7f9c204930),
    ('arbitrum', 0x66331f0b9cb30d38779c786bda5a3d57d12fba50),
    ('base', 0xa03be496e67ec29bc62f01a428683d7f9c204930),
    ('avalanche_c', 0xa03be496e67ec29bc62f01a428683d7f9c204930),
    ('polygon', 0x66331f0b9cb30d38779c786bda5a3d57d12fba50),
    ('bnb', 0xa03be496e67ec29bc62f01a428683d7f9c204930),
    ('lens', 0x798bb2d0ac591e34a4068e447782de05c27ed160)
) as t(blockchain, address) --noqa: LT01, AL05

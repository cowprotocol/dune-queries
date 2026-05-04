select
    'ethereum' as blockchain,
    *
from "query_6228775(blockchain='ethereum')"
where percent_of_reverts > 40.0
union all
select
    'gnosis' as blockchain,
    *
from "query_6228775(blockchain='gnosis')"
where percent_of_reverts > 40.0
union all
select
    'arbitrum' as blockchain,
    *
from "query_6228775(blockchain='arbitrum')"
where percent_of_reverts > 40.0
union all
select
    'base' as blockchain,
    *
from "query_6228775(blockchain='base')"
where percent_of_reverts > 40.0
union all
select
    'avalanche_c' as blockchain,
    *
from "query_6228775(blockchain='avalanche_c')"
where percent_of_reverts > 40.0
union all
select
    'polygon' as blockchain,
    *
from "query_6228775(blockchain='polygon')"
where percent_of_reverts > 40.0
union all
select
    'bnb' as blockchain,
    *
from "query_6228775(blockchain='bnb')"
where percent_of_reverts > 40.0
union all
select
    'linea' as blockchain,
    *
from "query_6228775(blockchain='linea')"
where percent_of_reverts > 40.0
union all
select
    'plasma' as blockchain,
    *
from "query_6228775(blockchain='plasma')"
where percent_of_reverts > 40.0
union all
select
    'ink' as blockchain,
    *
from "query_6228775(blockchain='ink')"
where percent_of_reverts > 40.0

# Dune Queries

Repository containing CoW DAO relevant dune queries.

## Developer Guide

Please employ standard engineering principles such as _divide and conquer_, _DRY (don't repeat yourself)_ by creating intermediate queries that can be used for debugging and depended on in upstream queries using Dune's [Query Views](https://docs.dune.com/query-engine/query-a-query#query-views)

For information on writing efficient Dune queries cf. [this guide](https://docs.dune.com/query-engine/writing-efficient-queries)

### Repository Structure

Queries can be placed in any subfolder (please organize the file structure thoughtfully).
All query file names must be formatted as `**/*_{queryId}.sql`.
This will cause continuous integration to automatically update queries in Dune whenever a PR is merged into main.

### Adding queries

In order to generate a new `queryId` create and save a new query via [https://dune.com/queries](https://dune.com/queries).
This will turn the url into something like `https://dune.com/queries/<some id>`, where the last part is your newly generated query id.
Upon merging your PR into main the content of the Dune query will be overridden by the content of the query file in github.

### Updating Queries

To update an existing query, simply change the SQL, then create, review and merge the PR. Upon merging the changes will be automatically synced to Dune.

### Removing Queries

Removing a content file does not automatically archive the query in Dune. If this is desired, please go ahead and remove it manually.

## Linting

[Sqlfluff](https://sqlfluff.com/) is run automatically on every PR to ensure queries follow a consistent formatting. See [this guide](https://docs.sqlfluff.com/en/stable/gettingstarted.html#installing-sqlfluff) for installing it locally.

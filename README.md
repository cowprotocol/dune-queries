# Balancer Dune Queries

Repository for managing Balancer protocol Dune Analytics queries. Changes merged to `main` are automatically synced to Dune.

## Repository Structure

```
balancer/                 Balancer protocol queries
  volume/                 Trading volume metrics
  tvl/                    Total Value Locked
  fees/                   Swap fees, protocol fees
  revenue/                Protocol revenue
  pools/                  Pool-level analytics
    overview/               Pool listings, general stats
    weighted/               Weighted pools
    stable/                 Stable / composable stable pools
    boosted/                Boosted pools (ERC-4626)
    lbp/                    Liquidity Bootstrapping Pools
  liquidity/              LP analytics, yield, impermanent loss
  governance/             veBAL, gauges, voting incentives
  token/                  BAL token supply, distribution, price
  dashboards/             Dashboard-specific composite queries
  views/                  Shared intermediate queries (Query Views)
cowamm/                   CoW AMM queries (Balancer CoW AMM product)
uploads/                  CSV files uploaded as Dune tables
scripts/                  Tooling for query management
```

All query file names follow the pattern `descriptive_name_{queryId}.sql`. The query ID is the numeric ID from the Dune URL (`dune.com/queries/{id}`).

## Quick Start

### Prerequisites

- Python 3.9+
- A Dune API key from a **Plus plan** or higher (create one at [Dune team settings](https://dune.com/settings/teams))

### Local Setup

```bash
cp .env.test .env             # copy template and fill in your DUNE_API_KEY
pip install -r scripts/requirements.txt
```

### Scripts

| Script | Description | Command |
|--------|-------------|---------|
| `pull_from_dune.py` | Fetch queries from Dune into the repo based on `queries.yml` | `python scripts/pull_from_dune.py` |
| `push_to_dune.py` | Push all managed queries from repo to Dune (manual full sync) | `python scripts/push_to_dune.py` |
| `preview_query.py` | Run a query and display the first 20 rows (uses API credits) | `python scripts/preview_query.py <query_id>` |
| `upload_to_dune.py` | Upload CSV files from `uploads/` to Dune as tables | `python scripts/upload_to_dune.py` |
| `validate.py` | Check naming conventions, manifest consistency, dependencies | `python scripts/validate.py` |

## Adding a New Query

1. Create the query on [dune.com](https://dune.com/queries) and save it. Note the query ID from the URL.
2. Add the ID to `queries.yml` with the target category:
   ```yaml
   - id: 1234567
     category: volume
   ```
3. Pull it into the repo:
   ```bash
   python scripts/pull_from_dune.py
   ```
   Or create the file manually: `balancer/volume/descriptive_name_1234567.sql`
4. Ensure the file starts with `-- part of a query repo` (the pull script adds this automatically).
5. If the query uses new Jinja parameters (e.g., `{{pool_type}}`), add them to the relevant `.sqlfluff` context file.
6. Test locally:
   ```bash
   sqlfluff lint balancer/volume/descriptive_name_1234567.sql
   python scripts/preview_query.py 1234567
   ```
7. Open a PR. SQLFluff runs automatically. Follow the PR template to document your changes.
8. On merge to `main`, CI automatically syncs the query to Dune.

## Updating Queries

Edit the SQL file, open a PR, and merge. CI handles the rest.

## Removing Queries

Deleting a file from the repo does **not** archive the query on Dune. If you want to archive it, do so manually on dune.com. Also remove the ID from `queries.yml`.

## Uploading CSV Tables

Place CSV files in the `uploads/` directory. On merge to `main`, they are uploaded to Dune as tables named `dune.{team_name}.dataset_{filename}` (without the `.csv` extension).

## Query Composition (DRY)

Use Dune [Query Views](https://docs.dune.com/query-engine/query-a-query#query-views) to avoid duplicating logic. Place shared intermediate queries in `balancer/views/` and reference them via `query_{id}` in downstream queries.

For tips on writing efficient queries, see the [Dune guide](https://docs.dune.com/query-engine/writing-efficient-queries).

## CI/CD

- **On PR**: [SQLFluff](https://sqlfluff.com/) lints changed SQL files. The [nitpicker](https://github.com/ethanis/nitpicker) bot flags common issues (e.g., using deprecated `prices.usd`, missing partition filters).
- **On merge to `main`**: Changed `.sql` files are synced to Dune via the [`bh2smith/dune-update`](https://github.com/bh2smith/dune-update) GitHub Action. Changed CSVs in `uploads/` are uploaded via `upload_to_dune.py`.

### Important Notes

- **Ownership**: Queries must be owned by the team whose API key is configured. You cannot update queries owned by other teams.
- **Rollback**: If a bad merge pushes broken SQL, use [Dune's query version history](https://dune.com/docs/app/query-editor/version-history) to revert.
- **File names are not synced**: Renaming a file in the repo does not rename the query on Dune. The `_{queryId}.sql` suffix is what matters -- do not remove it.

## Linting

[SQLFluff](https://sqlfluff.com/) runs on every PR. Install locally:

```bash
pip install sqlfluff
sqlfluff lint balancer/          # lint all Balancer queries
sqlfluff lint cowamm/            # lint CoW AMM queries
sqlfluff fix <file>              # auto-fix a specific file
```

## Contributing

See the [PR template](.github/PULL_REQUEST_TEMPLATE.md) for the required format. Issues can be filed using the [issue templates](.github/ISSUE_TEMPLATE/).

| Issue Type | Use For |
|------------|---------|
| Bug | Data quality issues, broken queries, miscalculations |
| Chart Improvement | Visualization suggestions |
| Query Improvement | SQL enhancements, new columns, performance |
| Question | General questions or suggestions |

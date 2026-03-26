**Is this linked to an existing issue?**
If so, link that issue(s) here.

**Fill out the following table describing your edits:**

| Original | Updated | Change | Reasoning |
|----------|---------|--------|-----------|
| [query_id](https://dune.com/queries/query_id) | [new_id](https://dune.com/queries/new_id) | What changed | Why |

**Provide any other context or screenshots that explain or justify the changes above:**

---

**Checklist:**

- [ ] SQL file name follows `descriptive_name_{queryId}.sql` convention
- [ ] File includes `-- part of a query repo` header
- [ ] Query ID is listed in `queries.yml`
- [ ] SQLFluff passes locally (`sqlfluff lint <file>`)
- [ ] If adding a new query: created on dune.com first, then added to repo
- [ ] If adding new Jinja parameters: updated the relevant `.sqlfluff` context

# migration

Generate fake data for the `card`, `business`, and `budget` tables based on sample CSVs, and emit a `load.sql` file with COPY or \copy statements for fast imports.

## Install

```bash
bun install
```

## Generate data

```bash
bun run index.ts --rows 5000 --out-dir ./out
```

Target a large card volume with a cap per business:

```bash
bun run index.ts --cards 100000 --max-cards-per-business 1000 --out-dir ./out
```

Example using the compiled binary in the same folder as the CSVs:

```bash
./data-generator \
  --card-file "./card.csv" \
  --business-file "./business.csv" \
  --budget-file "./budget.csv" \
  --enum-file "./enums.json" \
  --cards 100000 \
  --max-cards-per-business 1000 \
  --out-dir "./out"
```

Notes:
- The input CSVs in `from-db/` are required but gitignored; keep them locally (they are not committed).
- `from-db/enums.json` is required and used by default for enum columns.
- The 3 CSVs are exported from the original DB (via DBeaver export to CSV with header).
- If `--cards` is set and `--business-rows` is not, business rows are derived as `ceil(cards / max-cards-per-business)`.
- If `--budget-rows` is not set, budgets default to 2 per business (matching the default row ratios).
- Budgets are generated with `parent_budget_id = NULL`, `root_budget_id = id` for a simple hierarchy.
- Use `--progress-every` to see progress during long runs.

Outputs:
- `out/card.csv`
- `out/business.csv`
- `out/budget.csv`
- `out/load.sql`

Default behavior uses client-side `\copy` (portable paths):

```bash
bun run index.ts
```

Export enum values from the local DB and save to `from-db/enums.json`:

```bash
psql "postgresql://local:localpass@localhost:5432/migration_poc" -At -c "
WITH enum_cols AS (
  SELECT c.table_name,
         c.column_name,
         jsonb_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
  FROM information_schema.columns c
  JOIN pg_type t ON t.typname = c.udt_name
  JOIN pg_enum e ON e.enumtypid = t.oid
  WHERE c.table_schema = 'public'
  GROUP BY c.table_name, c.column_name
)
SELECT jsonb_object_agg(table_name || '.' || column_name, enum_values)
FROM enum_cols;
" > from-db/enums.json
```

`from-db/enums.json` is used automatically; override with `--enum-file` if needed.

Load into Postgres (psql on the host, connects to Docker):

```bash
psql "postgresql://local:localpass@localhost:5432/migration_poc" -f out/load.sql
```

If you prefer server-side `COPY`, mount the output directory into the container and use `--copy-mode copy` so Postgres can read the CSV paths.

## Build a binary

```bash
bun run build:bin
```

Run the binary:

```bash
./dist/data-generator --rows 5000 --out-dir ./out
```

## Local Postgres (Docker)

Build the PG18 image:

```bash
docker build -t migration-postgres .
```

Run a local instance:

```bash
docker run --name migration-postgres \
  -e POSTGRES_USER=local \
  -e POSTGRES_PASSWORD=localpass \
  -e POSTGRES_DB=migration_poc \
  -p 5432:5432 \
  -d migration-postgres
```

Verify version:

```bash
psql "postgresql://local:localpass@localhost:5432/migration_poc" -c "SHOW server_version;"
```

Enable required extensions:

```bash
psql "postgresql://local:localpass@localhost:5432/migration_poc" -c \
  "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"; CREATE EXTENSION IF NOT EXISTS ltree;"
```

## Create pg_restore dump from generated data

Generate, load, and export a custom-format dump (replicable):

```bash
bun run index.ts --cards 10000000 --max-cards-per-business 1000 --out-dir ./out
psql "postgresql://local:localpass@localhost:5432/migration_poc" -f out/load.sql
```

Run `pg_dump` locally (requires matching major version):

```bash
pg_dump "postgresql://local:localpass@localhost:5432/migration_poc" \
  --data-only -Fc -f ./out/fake_data.dump
```

Restore the generated dump:

```bash
pg_restore -d "postgresql://local:localpass@localhost:5432/migration_poc" ./out/fake_data.dump
```

## pg_dump from the original database

```bash
pg_dump "postgresql://USER:PASSWORD@HOST:5432/DBNAME" \
  --schema-only -Fc \
  --exclude-extension=pglogical \
  -f schema.dump

# data only for 3 tables
pg_dump "postgresql://USER:PASSWORD@HOST:5432/DBNAME" \
  --data-only -Fc \
  -f data.dump \
  -t public.card \
  -t public.business \
  -t public.budget
```


```bash
# generate list file
pg_restore -l schema.dump > schema.list
```

Edit `schema.list` to keep only:
- `SCHEMA - public`
- `EXTENSION - ltree`
- `EXTENSION - uuid-ossp`
- ...ENUMS used by the three tables
- `TABLE public.card`, `TABLE public.business`, `TABLE public.budget`

Then restore:

```bash
# clean target (optional)
psql "postgresql://local:localpass@localhost:5432/migration_poc" \
  -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

# schema (only selected objects)
pg_restore --section=pre-data --no-owner --no-privileges \
  -L schema.list \
  -d "postgresql://local:localpass@localhost:5432/migration_poc" schema.dump

# data (disable triggers for circular FKs)
pg_restore --section=data --disable-triggers --no-owner --no-privileges \
  -d "postgresql://local:localpass@localhost:5432/migration_poc" data.dump
```


Restore into the local Postgres container:

```bash
pg_restore -d "postgresql://local:localpass@localhost:5432/migration_poc" \
  -j 4 \
  reapcard.dump
```

## Notes
- Default input files are in `from-db/`: `card.csv`, `business.csv`, `budget.csv`, `enums.json`.

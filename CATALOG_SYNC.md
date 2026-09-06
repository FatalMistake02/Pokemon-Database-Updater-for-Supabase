# Public catalog database synchronization

The public card catalog is built in the separate
`Cardlyapp/cards-database` repository. Its `database-updater.py` uses
pluggable adapters under `sources/`; TCGdex is currently the source for both
international and Japanese data.

This repository only loads a completed, hash-verified public catalog into
Supabase and Neon. It does not generate or publish public catalog files.

## Publication boundary

The catalog repository performs these operations in order:

1. Generate all international and Japanese JSON files.
2. Write `manifest.json` after generation finishes.
3. Commit the completed files.
4. Deploy that exact version to GitHub Pages.
5. Finish successfully with a public workflow status and completed manifest.

The public build is scheduled at 00:17 and 12:17 UTC. `Sync Published Card
Database` starts three hours later at 03:17 and 15:17 UTC. It checks the public
workflow status every five minutes and waits up to three additional hours if
the build is queued or still running. It proceeds only after a successful
build and Pages deployment.

The sync reads the completed manifest's `updateType` and exact version, then
verifies every downloaded file against its SHA-256 hash before writing any
database rows. No cross-repository token is required.

New TCGdex releases bulk-load sets, cards, and prices. Routine publications
load prices only. Both modes update Supabase and all configured Neon targets in
batches of 100 rows.

## Setup

In `Cardlyapp/cards-database`, enable GitHub Pages with **GitHub Actions** as
the deployment source.

In this repository, set `CARD_CATALOG_URL` only if the Pages base URL differs
from `https://cardlyapp.github.io/cards-database`.

## Manual synchronization

The workflow can be run manually in full or price-only mode. An optional exact
catalog version can be supplied. From the command line:

```bash
python pokemon-db-updater.py \
  --catalog-url https://cardlyapp.github.io/cards-database \
  --expected-catalog-version MANIFEST_VERSION \
  --db-target both \
  --version both \
  --batch-size 100
```

Add `--prices-only` to update only prices.

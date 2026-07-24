# databricks-geobrix-raster

Agent skill for processing **raster geospatial data on Databricks** with [GeoBrix](https://databrickslabs.github.io/) RasterX — the Databricks Labs successor to Mosaic.

**Repo:** [github.com/beaxyz/databricks-geobrix-raster](https://github.com/beaxyz/databricks-geobrix-raster)

The agent loads [`SKILL.md`](SKILL.md) when a user's task involves gridded spatial data — even if they don't mention GeoBrix by name.

## When to use this skill

Use it whenever the task is **raster-first**: each pixel has a value at a geographic location.

**Good fits:**

- **GeoBrix setup** — install Lightweight or Heavyweight on a cluster
- **Reading rasters** — GeoTIFF (`.tif`/`.tiff`), NetCDF (`.nc`), GRIB
- **Earth observation** — Sentinel-2, Landsat, MODIS, VIIRS, Planet, etc.
- **Spectral indices** — NDVI, NDWI, EVI, NBR
- **Transformations** — clip to a boundary, reproject, mosaic, convert to COG
- **Aggregation** — pixel values to H3 hex cells or zonal stats over polygons
- **Large scenes** — retile-and-persist for multi-GB GeoTIFFs

**Typical trigger phrases:**

- *"I have a TIFF at…"* / *"process this satellite image"* / *"compute NDVI"*
- *"Aggregate this raster to H3"* / *"mean elevation per county"*
- *"Reproject this raster"* / *"clip the raster to…"*

**Do not use** for pure vector work (points/lines/polygons without rasters) or H3 on point data alone — use native DBSQL `ST_` / `H3_*` functions instead.

GeoBrix officially supports **GeoTIFF** for production ingest. NetCDF/GRIB are best-effort via GDAL subdatasets — see [`references/2-ingest/2d-netcdf-grib.md`](references/2-ingest/2d-netcdf-grib.md).

## Using this skill

**Local (Claude Code / Cursor):** drop this folder into your agent skills directory (e.g. `~/.claude/skills/`).

**Databricks Genie / Assistant:** sync to workspace skills:

```bash
cd databricks-geobrix-raster

databricks sync . \
  "/Workspace/Users/<you>/.assistant/skills/databricks_geobrix_raster" \
  --full
```

Replace `<you>` with your workspace user path. Re-run sync after edits.

## Execution tiers

GeoBrix ships two tiers with the same `rst_*` analytics API:

| | **Lightweight** (default) | **Heavyweight** |
|---|---|---|
| Package | `pyrx` | `rasterx` |
| Compute | **Serverless** (preferred) or classic | Classic **x86** only |
| Install | `%pip [light]` wheel | JAR + GDAL init script + WHL |
| GeoTIFF reader | `gtiff_gbx` | `gtiff_gdal` |
| Doc | [`references/1-install/install-light.md`](references/1-install/install-light.md) | [`references/1-install/install-heavy.md`](references/1-install/install-heavy.md) |

**Defaults:** Lightweight on Serverless. Route to Heavyweight for OGR readers, PMTiles writer, exotic GDAL options, or when `pyrx` is unavailable.

Full routing rules → `SKILL.md` → **Execution tier selection**.

## How the skill works

`SKILL.md` is the source of truth. Flow:

1. **Execution tier selection** — pick tier + compute + readers (canonical rulebook)
2. **Substitution policy** — if GeoBrix is not installed: recommend tier, ask user (a) Light / (b) Heavy / (c) fallback, wait for choice
3. **Workflow**
   - **Phase 1** — detect compute, bootstrap GeoBrix
   - **Phase 2a** — mandatory size check (before any read)
   - **Phase 2b / 2c / 2d** — read path by format and size (standard / large GeoTIFF / NetCDF-GRIB)
   - **Phase 3** — analytics on the `tile` column (`rst_*`)
   - **Phase 4** — persist to Delta in Unity Catalog

## Prerequisites

| | Lightweight (default) | Heavyweight |
|---|---|---|
| DBR | 17.3 LTS or 18 LTS | 17.1+ |
| Python | 3.12 (Serverless env **5+** on Serverless) | Per cluster DBR |
| UC Volume | WHL staging | JAR, `.so`, init script, WHL |

## Repository contents

| Path | Purpose |
|---|---|
| [`SKILL.md`](SKILL.md) | Agent skill — policies, tier routing, workflow phases |
| [`references/1-install/install-light.md`](references/1-install/install-light.md) | Lightweight install (Serverless, `%pip [light]`) |
| [`references/1-install/install-heavy.md`](references/1-install/install-heavy.md) | Heavyweight install (JAR, init script, WHL on classic x86) |
| [`references/functions.md`](references/functions.md) | `rx.*` function reference |
| [`references/todo.md`](references/todo.md) | Internal backlog — format support, pattern gaps, tier consistency |
| [`references/3-process/analytics.md`](references/3-process/analytics.md) | Phase 3 stats, clip, zonal (format-agnostic) |
| [`references/3-process/h3.md`](references/3-process/h3.md) | Phase 3 H3 tessellation and aggregation |
| [`references/2-ingest/2c-large-raster-retile.md`](references/2-ingest/2c-large-raster-retile.md) | Phase 2c large GeoTIFF retile-and-persist |
| [`references/2-ingest/2d-netcdf-grib.md`](references/2-ingest/2d-netcdf-grib.md) | Phase 2d NetCDF/GRIB subdataset flow |

## Lightweight bootstrap (reference)

After `%pip` install and `restartPython()`:

```python
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx import functions as rx

register(spark)       # *_gbx readers (gtiff_gbx, raster_gbx)
rx.register(spark)    # rst_* functions
```

Heavyweight: `from databricks.labs.gbx.rasterx import functions as rx` then `rx.register(spark)` only (JAR registers readers).

## Outstanding (deferred)

Backlog — format support, pattern gaps (visualization), and tier-consistency work — is tracked in [`references/todo.md`](references/todo.md).

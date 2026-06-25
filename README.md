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

GeoBrix officially supports **GeoTIFF** for production ingest. NetCDF/GRIB are best-effort via GDAL subdatasets — see [`references/examples/netcdf-ingest.md`](references/examples/netcdf-ingest.md).

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
| Doc | [`references/install-light.md`](references/install-light.md) | [`references/install-heavy.md`](references/install-heavy.md) |

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
| [`references/install-light.md`](references/install-light.md) | Lightweight install (Serverless, `%pip [light]`) |
| [`references/install-heavy.md`](references/install-heavy.md) | Heavyweight install (JAR, init script, WHL on classic x86) |
| [`references/functions.md`](references/functions.md) | `rx.*` function reference |
| [`references/examples/raster-analytics.md`](references/examples/raster-analytics.md) | Phase 3 stats, clip, zonal (format-agnostic) |
| [`references/examples/h3-examples.md`](references/examples/h3-examples.md) | Phase 3 H3 tessellation and aggregation |
| [`references/examples/large-raster-retile.md`](references/examples/large-raster-retile.md) | Phase 2c large GeoTIFF retile-and-persist |
| [`references/examples/netcdf-ingest.md`](references/examples/netcdf-ingest.md) | Phase 2d NetCDF/GRIB subdataset flow |

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

- Light tier bootstrap + reader swaps in example docs and tier-aware Phase 2b in `SKILL.md` — Phase 3 analytics unchanged; use `GEOTIFF_READER` / `GENERIC_READER` from Phase 1
- NetCDF/GRIB upfront tier routing and xarray→GeoTIFF→light path (`netcdf-ingest.md` remains heavy-only for now)

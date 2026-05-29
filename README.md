# databricks-geobrix-raster

A Claude/Cursor agent skill for processing **raster geospatial data on Databricks** using the
[GeoBrix](https://databrickslabs.github.io/) `RasterX` library — the next-generation Databricks
Labs successor to Mosaic.

Use this skill when working with satellite imagery, elevation models, weather grids, land cover,
or any other gridded spatial data on Databricks.

## What this skill helps with
- **Reading raster files** — GeoTIFF (`.tif`/`.tiff`), NetCDF (`.nc`), GRIB
- **Earth observation imagery** — Sentinel-2, Landsat, MODIS, VIIRS, Planet, etc.
- **Spectral indices** — NDVI, NDWI, EVI, NBR, and other band-ratio computations
- **Raster transformations** — clip to polygon, reproject between CRSes, mosaic adjacent tiles, convert to COG
- **Aggregation** — pixel values to H3 hexagons or arbitrary polygons (zonal statistics)
- **Tiling and chunking** — split large scenes for parallel processing
- **Rasterization** — convert vector geometries into a grid
- **Cluster setup** — GDAL init script + JAR/WHL installation

## Repository contents
| File | Purpose |
|---|---|
| `SKILL.md` | The agent skill itself — guidance, decision policies, and code patterns |
| `references/install.md` | Step-by-step cluster install (init script, JAR, library wheel) |
| `references/functions.md` | Reference for the GeoBrix `rx.*` functions used in the skill |

## Prerequisites
- Databricks Runtime **17.1 or later** (LTS recommended)
- **Classic cluster** — Serverless is not supported
- A **Unity Catalog Volume** to host the JAR, `.so` file, and init script
- **Cluster admin permissions** to attach init scripts and libraries
Full setup steps live in `references/install.md`.

## Using this skill
Drop this folder into your agent's skills directory (e.g., `~/.claude/skills/` for Claude Code,
or wherever your tooling expects skills). The agent will load `SKILL.md` automatically when a
user's task matches a raster-related trigger phrase.
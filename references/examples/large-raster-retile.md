# Pattern: Large raster ingestion (retile-and-persist)

**This is where Phase 2c routes when the size check in 2a flags the source as LARGE** — global scenes, multi-band imagery, or anything where the input is more than ~5–10 GB. The standard Phase 2 → Phase 3 flow (read, operate, write) doesn't scale well to global-coverage rasters because every downstream op re-reads the source. For scenes like VIIRS / Landsat / Sentinel mosaics, **read → retile → persist → operate** is the production pattern.

**Scope — single-grid rasters only.** This pattern applies to formats that present as one georeferenced grid with real bands and a real CRS: **GeoTIFF / COG**, JPEG2000 (`.jp2`), ERDAS IMAGINE (`.img`), and similar. It does **NOT** apply to **NetCDF or GRIB** — those expose variables as subdatasets and frequently carry no embedded CRS, so their read + metadata steps differ (see the NetCDF example). COG needs no special handling: it's a GeoTIFF and reads through the `GTiff` driver (there is no read-time "COG" driver).

## Why the simple pattern fails at scale

| Symptom | Cause |
|---|---|
| First operation hangs for many minutes | GDAL has to scan and split the giant file every time |
| Driver OOM on metadata extraction | Catalog enumeration over many splits in a single call |
| Downstream ops re-read the source TIFF | Without persisting, Spark recomputes the split for each transform |
| Long tail on a few executors | `sizeInMB=16` splits are MB-bounded, not pixel-bounded — uneven tile shapes |

## Step-by-step

```python
import math
from pyspark.sql import functions as F
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

src_path = "${src_path}"   # single-grid raster: GeoTIFF/COG, .jp2, .img …
retiled_table = "${catalog}.${schema}.retiled_raster"

# 1. Read the raster — pick the GDAL driver from the extension. The reader splits the
#    file by `sizeInMB` (default 16) so each row in `raster_df` represents one CHUNK,
#    not the whole file.
#
#    SINGLE-GRID formats only. COG is not a special case — it's a GeoTIFF and reads
#    through `GTiff`. NetCDF/GRIB are NOT handled here (variables exposed as
#    subdatasets, often no embedded CRS) — use the NetCDF example instead.
ext = src_path.lower().rsplit(".", 1)[-1]
single_grid_drivers = {
    "tif": "GTiff", "tiff": "GTiff",   # GeoTIFF / COG
    "jp2": "JP2OpenJPEG",              # JPEG2000
    "img": "HFA",                      # ERDAS IMAGINE
}
if ext in ("nc", "grib", "grib2"):
    raise ValueError(
        f".{ext} is a subdataset/multidimensional format — use the NetCDF example, "
        "not this single-grid retile pattern."
    )
driver = single_grid_drivers.get(ext, "GTiff")  # unknown single-grid ext → default GTiff

raster_df = (
    spark.read.format("gdal")
        .option("driverName", driver)
        .load(src_path)
)

# 2. Inspect TRUE source dimensions.
#    ⚠️ CRITICAL: `rx.rst_width("tile")` on a single row returns the CHUNK's width,
#    not the source file's width. DO NOT use `.limit(1).collect()` for sizing —
#    you'd be sizing tiles to the chunk, not the file, and downstream retile would
#    produce thousands of tiny tiles instead of ~200.
#
#    Use ONE of the two methods below to get true full-file dimensions.

# --- Option A (preferred — rasterio for metadata only) ---
# rasterio reads the GTiff/NetCDF/etc. header without scanning pixel data, returning
# true file dimensions instantly. This is a LEGITIMATE use of rasterio (metadata-only
# header read) and is an accepted exception to the substitution policy because
# GeoBrix still does the heavy work (the retile + persist + all downstream Spark ops).
# Empirically faster than the GeoBrix aggregation alternative on most clusters,
# because it's a single header read on the driver instead of a distributed UDF pass.
import rasterio
with rasterio.open(src_path) as src:
    width, height = src.width, src.height
    bands = src.count
    srid = src.crs.to_epsg() if src.crs else None
print(f"Full raster: {width} × {height} px, {bands} band(s), EPSG:{srid}")

# --- Option B (alternative — GeoBrix-only single-pass aggregation) ---
# If you need to avoid rasterio entirely (e.g., it's not installed, or the source
# format isn't well-supported by rasterio), use this aggregation pattern:
# union per-chunk extents to reconstruct the file extent. Mathematically correct
# but typically slower than Option A because it runs UDFs on every chunk.
# Note: `pixelheight` is NEGATIVE for north-up rasters; the ymin formula handles
# the sign correctly via (uly + chunk_h * px_h).
#
# full_extent = raster_df.select(
#     rx.rst_upperleftx("tile").alias("ulx"),
#     rx.rst_upperlefty("tile").alias("uly"),
#     rx.rst_width("tile").alias("chunk_w"),
#     rx.rst_height("tile").alias("chunk_h"),
#     rx.rst_pixelwidth("tile").alias("px_w"),
#     rx.rst_pixelheight("tile").alias("px_h"),
#     rx.rst_numbands("tile").alias("bands"),
#     rx.rst_srid("tile").alias("srid"),
# ).select(
#     F.min("ulx").alias("xmin"),
#     F.max(F.col("ulx") + F.col("chunk_w") * F.col("px_w")).alias("xmax"),
#     F.min(F.col("uly") + F.col("chunk_h") * F.col("px_h")).alias("ymin"),
#     F.max("uly").alias("ymax"),
#     F.first("px_w").alias("px_w"),
#     F.first("px_h").alias("px_h"),
#     F.first("bands").alias("bands"),
#     F.first("srid").alias("srid"),
# ).collect()[0]
# width  = int(round((full_extent.xmax - full_extent.xmin) / full_extent.px_w))
# height = int(round((full_extent.ymax - full_extent.ymin) / abs(full_extent.px_h)))
# bands, srid = full_extent.bands, full_extent.srid
#
# Do NOT use `rst_width` from a single chunk row — that's the bug this section
# exists to prevent.

# 3. Derive tile size from raster shape — DO NOT hardcode.
#    Target a total tile count that parallelizes well without overhead bloat.
#    Rule of thumb: 100-500 total tiles is a reasonable range for most clusters.
target_total_tiles = 200
ideal_tile_px = int(math.sqrt((width * height) / target_total_tiles))

# Round to a common tile size in the [512, 16384] range for memory predictability
common_sizes = [512, 1024, 2048, 4096, 8192, 16384]
tile_size = min(common_sizes, key=lambda s: abs(s - ideal_tile_px))
estimated_tiles = math.ceil(width / tile_size) * math.ceil(height / tile_size)
print(f"Tile size: {tile_size} × {tile_size}, estimated ~{estimated_tiles} tiles")

# 4. Retile + persist to Delta. The persist step is what makes downstream cheap —
#    every later op reads the materialized tiles, not the giant TIFF.
retiled_df = raster_df.withColumn(
    "retiled", rx.rst_retile("tile", F.lit(tile_size), F.lit(tile_size))
)
retiled_df.write.mode("overwrite").saveAsTable(retiled_table)

# 5. Read back from the persisted table for ALL downstream work
tiles = (
    spark.read.table(retiled_table)
    .drop("tile")
    .withColumnRenamed("retiled", "tile")
)
```

## Tuning tile size

The auto-derived size targets ~200 total tiles. Override `target_total_tiles` if you have specific needs:

| Trade-off | Increase target tiles | Decrease target tiles |
|---|---|---|
| Parallelism | More tiles, more parallel work | Fewer tiles, less parallel |
| Per-tile work | Smaller tiles, less work each | Larger tiles, more work each |
| File / metadata overhead | More files in Delta, higher overhead | Fewer files, less overhead |
| Memory per tile | Lower (smaller tile fits comfortably) | Higher (large tile may OOM) |

For a cluster with ~4 workers, ~200 tiles gives each worker ~50 tiles — a good default. Tune higher (500+) for very large clusters; tune lower (100) if per-tile ops are expensive.

## After retile: downstream analytics

The `tiles` DataFrame has a `tile` column — identical in shape to a Phase 2b standard read. From here it's **Phase 3**, format-agnostic: see `references/examples/raster-analytics.md` (stats, clip, zonal) and `references/examples/h3-examples.md` (H3 tessellation/aggregation, including the single-band `[0]` band-index gotcha).

## When NOT to use this pattern

- Small rasters (< 1 GB) — the persist-to-Delta overhead isn't worth it. Stick to the standard Phase 2–4 flow.
- Single-tile operations (one polygon, one zonal stat) — the metadata-then-clip path is faster.
- Truly streaming workloads — this pattern is batch-oriented; for continuous ingest, you'd structure differently.

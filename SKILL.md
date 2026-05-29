---
name: databricks-geobrix-raster
description: Process raster geospatial data on Databricks — satellite imagery, elevation models, weather grids, nighttime lights, land cover, and other gridded spatial data. Use when reading GeoTIFF (.tif/.tiff), NetCDF (.nc), or GRIB files; computing vegetation/water/burn indices like NDVI/NDWI/NBR; clipping rasters to polygons or country/city boundaries; reprojecting between coordinate systems (CRS, EPSG); aggregating raster pixel values to H3 hexagons or zones (zonal statistics — mean/sum/max per region); mosaicking/stitching adjacent tiles; tiling large scenes for parallel processing; or working with imagery from Sentinel, Landsat, MODIS, VIIRS, or other earth observation missions. Implementation uses the GeoBrix RasterX library (successor to Mosaic) on a classic Databricks cluster. Triggers on terms like raster, GeoTIFF, .tif, satellite imagery, NDVI, NDWI, vegetation index, zonal stats, elevation, DEM, DTM, Sentinel, Landsat, VIIRS, MODIS, nighttime lights, land cover, H3 raster aggregation, raster to hex, clip raster, reproject raster, mosaic raster, rasterize, GeoBrix, RasterX, GDAL, spatial raster.
---

# Databricks GeoBrix Raster

Process raster geospatial data on Databricks using the GeoBrix library — the next-generation Databricks Labs replacement for Mosaic. Covers GeoTIFF ingestion, raster transformations (clip, reproject, mosaic), spectral indices (NDVI), H3 grid aggregation, and zonal statistics.

## When to Use This Skill

Apply this skill whenever a user task involves **raster data** — gridded spatial data where each pixel has a value at a geographic location. Users typically describe this in domain terms; recognize the pattern even when GeoBrix isn't mentioned by name.

Use this skill when the user is working on:

- **Reading raster files** — anything with a `.tif`/`.tiff`/`.nc`/`.grib` extension; "GeoTIFF", "NetCDF", "GDAL"; satellite imagery, scientific data formats
- **Earth observation imagery** — Sentinel-2, Landsat, MODIS, VIIRS, Planet, commercial satellite providers
- **Vegetation / water / fire indices** — NDVI, NDWI, EVI, NBR, any "(band − band) / (band + band)" computation
- **Elevation models** — DEM (Digital Elevation Model), DTM (Digital Terrain Model), DSM
- **Land cover / land use rasters** — categorical pixel data describing what each location is
- **Weather and climate grids** — temperature, precipitation, wind grids over time
- **Nighttime lights / urbanization** — VIIRS DNB, DMSP-OLS, anthropogenic activity datasets
- **Raster transformations** — clipping to a polygon/boundary, reprojection between coordinate systems (CRS/EPSG codes), mosaicking adjacent tiles, format conversion to COG
- **Aggregating raster pixels to regions** — either *H3 cells* (uniform hex grid) or *arbitrary polygons* (zonal stats: mean elevation per county, total rainfall per watershed, etc.)
- **Tiling / chunking** large scenes for parallel processing
- **Rasterizing** vector geometries into a grid
- **Cluster setup** for GeoBrix (GDAL init script, JAR + WHL install)

Trigger phrases that indicate this skill, even without "GeoBrix":
- "I have a TIFF file at..." / "process this satellite image" / "compute NDVI on..."
- "Aggregate this raster to H3" / "raster to hex" / "mean value per polygon"
- "Reproject this raster" / "clip the raster to..." / "my rasters are in different CRSes"
- "Sentinel-2 / Landsat / VIIRS / MODIS [anything]"
- "Elevation per [region]" / "land cover percent per [zone]"

Do NOT use this skill for:
- Pure vector geospatial work (points, lines, polygons without rasters) — use native DBSQL `ST_` functions (covered in `databricks-dbsql`)
- H3 indexing of point/polygon data alone (no raster involved) — use native DBSQL H3 functions

## Substitution policy — GeoBrix is the default. STOP and ASK before any fallback.

**When this skill is loaded and the user has a raster task, use GeoBrix.** Do NOT substitute another library (rasterio, gdal CLI, geopandas, etc.) just because GeoBrix isn't installed yet. Substituting bypasses the entire reason this skill exists (Spark-native distributed processing, UC integration, scale beyond a single node).

**If GeoBrix is not installed on the cluster:**

1. **Tell the user explicitly:** "GeoBrix isn't installed on this cluster."
2. **Default recommendation is to install GeoBrix.** Offer to walk through `references/install.md` (pre-flight checks + parameter collection + setup). This is the path that respects the user's intent in invoking the skill.
3. **STOP. Do not proceed with any code execution.** Ask the user a direct yes/no question and wait for their answer:
   > *"GeoBrix isn't installed. I can walk you through the install (init script + cluster restart), or fall back to a different library for this one task. Want me to: **(a) install GeoBrix**, or **(b) use a non-GeoBrix fallback for this single task only**?"*
4. **Do NOT infer consent.** A prompt that mentions "exploratory" or "one-off" or "just for this notebook" is not consent. The user asking the original question is not consent. Only an explicit "yes, use a fallback" or "yes, option b" from the user is consent.
5. **If — and only if — the user explicitly picks (b)**, proceed with rasterio (or appropriate alternative). Always flag it in the response: *"Using rasterio for this one task per your confirmation. GeoBrix is the right tool for production / multi-file / large-raster work — install when ready."*
6. **If the user picks (a)**, walk through `references/install.md` from the parameter-collection step onwards.

**Hard rules:**
- Never decide "all fallback conditions are met" on your own and proceed. That's the LLM inferring consent. The skill explicitly forbids it.
- Never write code that uses rasterio / gdal / geopandas for **raster processing** until the user has confirmed (b) in this conversation.
- Never frame the fallback as "since this is a one-off, I'll use rasterio." Frame it as a choice: "Want me to install GeoBrix or fall back?"

The user invoked this skill because they want GeoBrix. Respect that until they explicitly say otherwise.

### Where rasterio IS allowed in this skill (the only exceptions)

The substitution policy above forbids rasterio as a stand-in for GeoBrix in the **processing path**. Two narrow exceptions are explicitly allowed because rasterio is the right tool for them — GeoBrix isn't designed to compete:

| Use case | Allowed? | Why |
|---|---|---|
| **Metadata-only header read** (Phase 2c Option A: get true `width`, `height`, `srid`, `bands` from the source file in a single header call) | ✅ Allowed | GeoBrix can't do this in one driver-side call without a distributed UDF pass over all chunks; rasterio reads only the header. GeoBrix still does the entire retile + persist + downstream work. |
| **Final-step visualization** (read the GTiff bytes produced by `rst_asformat("GTiff")` into a numpy array, render with matplotlib / contextily / etc.) | ✅ Allowed | GeoBrix returns raster tiles; matplotlib needs numpy arrays. rasterio is the standard bridge for byte-to-array conversion at the rendering step only. |
| **Reading the source raster for ingest** (instead of `spark.read.format("gdal")`) | ❌ Forbidden | This is the substitution the policy exists to prevent. |
| **Raster transformations** (clip, reproject, mosaic, NDVI, zonal stats) | ❌ Forbidden | All of these are GeoBrix-native; substituting rasterio loses parallelism and Spark integration. |
| **Replacing GeoBrix because "the file is small"** | ❌ Forbidden | Phase 2a's size check determines routing; size alone doesn't justify swapping libraries. |
| **Replacing GeoBrix because "GeoBrix isn't installed"** | ❌ Forbidden without explicit consent | This is the (b) escape valve in the substitution policy. Requires user opt-in. |

**Whenever you use rasterio under one of the allowed exceptions, surface it explicitly in your response:** "Using rasterio for [metadata read / final visualization]; GeoBrix is still doing the [retile / persist / clip / aggregation]." Don't be silent about it — the user should always see which library is doing what.

**Install requirement:** rasterio is not preinstalled on DBR. If using either allowed exception, install first:
```python
%pip install rasterio
dbutils.library.restartPython()
# After restart, re-import: from databricks.labs.gbx.rasterx import functions as rx; rx.register(spark)
```
Or add `rasterio` to the cluster's Libraries tab for repeated use.

## Background

GeoBrix is the successor to DBLabs Mosaic, modernized for the Data Intelligence Platform. It exposes three packages — but only **RasterX** is meaningfully the right tool for new work:

- **RasterX** — raster processing (the focus of this skill).
- **GridX** — discrete global grid indexing. For fresh **H3 work on point/polygon data**, use **native DBSQL `H3_*` functions** (covered in the `databricks-dbsql` skill). GridX is the right tool only for **BNG (British National Grid)** or **migrating Mosaic grid code**.
- **VectorX** — *not the right tool for general vector work.* It contains a single migration helper for converting legacy Mosaic geometries to native UC `GEOMETRY`/`GEOGRAPHY` types. For all other vector operations (`ST_Intersects`, buffers, spatial joins, geometry construction), use **native DBSQL `ST_` functions** (also covered in `databricks-dbsql`).

GeoBrix is Beta, runs **only on Databricks Runtime (classic clusters, not Serverless)**, and is community-maintained (AS-IS, no SLA). See [databrickslabs.github.io/geobrix](https://databrickslabs.github.io/geobrix/).

## Prerequisites

- **DBR 17.1 or later** (LTS releases recommended)
- **Classic cluster** (Serverless is not supported)
- **Unity Catalog Volume** to host the JAR, `.so`, and init script
- **Cluster admin permissions** to attach init scripts and libraries

Full setup steps: see `references/install.md`.

## Workflow

### Phase 1: Verify compute type, then verify GeoBrix is installed

**1a. Confirm you're on a classic interactive cluster.** GeoBrix does **not** run on Serverless compute or SQL warehouses — they don't support cluster-level init scripts or JAR installation.

In a notebook: click the **Connect** dropdown at the top right. The attached compute must be a **classic All-Purpose / Interactive cluster** (look for the cluster icon, not "Serverless" or "SQL warehouse"). If it isn't:
- Pick or create a classic cluster from the Connect dropdown
- Attach the notebook to it and re-run

If you're working from the **SQL Editor**, you can't run GeoBrix there at all — switch to a notebook attached to a classic cluster.

**1b. Verify GeoBrix is registered AND inspect the actual API surface (prevents hallucinated function names):**

```python
try:
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)

    n = spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_*'").count()
    print(f"✅ GeoBrix installed: {n} raster functions registered.")

    # Inspect the actual Python API surface. Use ONLY names that appear in this output
    # for any rx.<name>(...) call. Do not invent names (no read_raster, no load_tiff, etc.)
    api_names = sorted(name for name in dir(rx) if not name.startswith("_"))
    print(f"\nrx.<...> API (use only these names):")
    print(", ".join(api_names))
except Exception as e:
    print(f"❌ GeoBrix not installed or misconfigured: {e}")
```

**If functions are registered** → proceed to Phase 2.

**Hard rule on the API surface:**
- `rx` exposes only the names printed above (typically `register` + `rst_<...>` functions).
- **There is NO `rx.read_raster`, `rx.load_tiff`, `rx.from_path`, or any other "read" helper on the `rx` module.** Raster ingestion is always via `spark.read.format("gtiff_gdal" | "gdal").load(path)` (Phase 2b) or via the prescribed retile-and-persist pattern (Phase 2c). If you find yourself reaching for an `rx.read_*` or `rx.load_*` function, stop — it doesn't exist.
- **For function details (signature, purpose, category) → read `references/functions.md`.** It's the curated catalog of every `rst_*` function organized by category (metadata, transformations, generators, H3 aggregation, etc.).
- **DO NOT run `DESCRIBE FUNCTION EXTENDED gbx_rst_<name>` on each function** as a way to learn the API. It's slow (one SQL call per function), the output is Spark-formatted (not user-friendly), and the same information already lives in `references/functions.md`. The only valid use of `DESCRIBE FUNCTION EXTENDED` is debugging a specific signature mismatch at runtime.
- If you're unsure whether a function exists or what it does, in order: (1) check the printed `api_names` list above, (2) grep `references/functions.md`, (3) if both miss, the function doesn't exist — don't invent it.

**If the import or SQL check fails** on a classic cluster → the cluster needs bootstrap. See `references/install.md` for the full procedure (download artifacts → upload to UC Volume → configure init script + library on a classic DBR 17.1+ cluster).

Before running any install step, **collect parameters from the user** (Volume path, cluster ID, GeoBrix version) — `install.md` lists them up-front and the snippets won't work with placeholders. Once installed, re-run the check above.

### Phase 2: STOP. Run the size check before anything else.

**🛑 BEFORE writing any code that touches the source raster, you (Claude / Genie Code) must emit and run the `dbutils.fs.ls` size check below. This is not advisory. It is the first code cell of any GeoBrix raster session after Phase 1.**

Do not estimate the source size from the filename, the dataset name (e.g. "VIIRS"), prior conversation history, or general knowledge of typical satellite product sizes. Real numbers only.

#### What counts as "touching the source" — all of these are forbidden before Phase 2a

The rule is about **timing, not syntax**. Do not emit any of these against the source path until Phase 2a has run and its output is in the conversation:

- `spark.read.format("gtiff_gdal" | "gdal" | ...).load(source_path)` — the standard reader path
- `spark.sql("SELECT * FROM gbx_rst_maketiles('...')")` or any **SQL TVF** that takes the file path
- `rx.rst_fromfile(source_path)` / `gbx_rst_fromfile(source_path)` — direct constructor
- `rx.rst_fromcontent(...)` / `gbx_rst_fromcontent(...)` reading from `binaryFile`-loaded bytes of the source
- Any other GeoBrix function that takes a file path / URI to the source
- Any `dbutils.fs.head` / `binaryFile` / `spark.read.format("binaryFile")` of the source (these read bytes too)

The only operation allowed against the source path before Phase 2a runs is **`dbutils.fs.ls`** itself (it's metadata-only, no byte read).

#### Phase 2a. Size check (MANDATORY first action)

```python
items = dbutils.fs.ls(source_path)  # works on single file or directory

total_bytes = sum(item.size for item in items)
total_gb = total_bytes / (1024**3)
n_files = len(items)

if n_files == 1:
    print(f"Single file: {total_gb:.2f} GB")
    avg_mb = total_gb * 1024
else:
    avg_mb = total_bytes / n_files / (1024**2)
    print(f"{n_files} files, total {total_gb:.2f} GB, avg {avg_mb:.1f} MB/file")

# Routing decision — print loud and clear so the human and the LLM both see it
if total_gb > 5:
    print("\n*** ROUTING: LARGE ***")
    print("MUST use the 'Pattern: Large raster ingestion' below.")
    print("MUST NOT write any spark.read.format(...).load(source_path) call against the source.")
elif n_files > 100 and avg_mb > 500:
    print("\n*** ROUTING: LARGE (many large files) ***")
    print("MUST use the retile-and-persist pattern.")
else:
    print("\n*** ROUTING: SMALL/MEDIUM ***")
    print("Phase 2b standard read is appropriate.")
```

**After this cell runs, look at its output. The routing decision drives everything downstream.**

#### Hard rules — non-negotiable, no creative interpretation

1. **If routing is LARGE, you MUST jump to Phase 2c → retile-and-persist.** Skip Phase 2b entirely. Do not write `spark.read.format("gtiff_gdal").load(huge_source)`. Do not write a "smarter" optimization (see anti-patterns below). The retile-and-persist pattern is the prescribed path; there is no approved alternative.

2. **No "optimizations" that still read the source directly.** Variations like:
   - "Read with `sizeInMB` splitting, then filter tiles by `rst_boundingbox` intersection before clipping"
   - "Read once, clip to bbox, merge, then process"
   - "Read with a smaller `sizeInMB` to make it more manageable"

   ...are all the same anti-pattern. They all read the giant source on every operation. The retile-and-persist pattern is the only approved approach for LARGE sources.

3. **No reasoning around the size check.** Don't write "the file is probably small enough" or "since the output (clip to one city) will be small, we can skip retile-and-persist." The skill enforces routing based on **input** size, not anticipated output size.

4. **Thresholds are minimum-bars, not preferences.** When in doubt between standard read and retile-and-persist, choose retile-and-persist.

5. **You must surface the routing decision to the human.** After Phase 2a runs, your next message should state: "Phase 2a routed this as [LARGE / SMALL-MEDIUM]. Proceeding with [retile-and-persist / standard read]." Do not silently transition.

#### Phase 2b. Standard read (SMALL/MEDIUM routing only)

**Only enter this section if Phase 2a printed `ROUTING: SMALL/MEDIUM`.** Otherwise, jump to Phase 2c.

Pick the reader based on the source extension:

```python
ext = source_path.lower().rsplit(".", 1)[-1]

if ext in ("tif", "tiff"):
    rasters = spark.read.format("gtiff_gdal").load(source_path)
elif ext == "nc":
    rasters = spark.read.format("gdal").option("driverName", "NetCDF").load(source_path)
elif ext in ("grib", "grib2"):
    rasters = spark.read.format("gdal").option("driverName", "GRIB").load(source_path)
else:
    # Let GDAL auto-detect from extension
    rasters = spark.read.format("gdal").load(source_path)

# Optional: split per-file at a target size. Add only if files are several hundred MB+.
# .option("sizeInMB", "32")
```

The output has a `tile` column ready for RasterX functions. Proceed to Phase 3.

#### Phase 2c. LARGE routing → retile-and-persist

If Phase 2a printed `ROUTING: LARGE`, skip Phase 2b and jump to **Pattern: Large raster ingestion** later in this doc. That pattern is the single source of truth for LARGE sources; it handles read + inspect + auto-sized retile + Delta persist, and feeds downstream phases from the persisted table.

#### Anti-patterns — DO NOT EMIT THESE for LARGE sources

| Anti-pattern | Why it's wrong |
|---|---|
| `spark.read.format("gtiff_gdal").load(huge_file)` | Reads the giant file, often multiple times. The MB-bounded split is not a substitute for retile-and-persist. |
| `SELECT * FROM gbx_rst_maketiles('{huge_file}', ...)` as the ingest step | `rst_maketiles` is a **generator** (build tiles from an extent + grid definition), not a file-ingest function. Using it to ingest a TIFF is either a misuse or a workaround for the size-check gate. Use the prescribed read pattern (small/medium) or retile-and-persist (large). |
| `rst_fromfile(huge_file)` as the entry point | Same family as the SQL TVF above — a "direct constructor" that bypasses the size-check gate. Allowed only after Phase 2a routes the source as SMALL/MEDIUM. |
| Read source twice (e.g. once for metadata, once for clipping) | Every downstream op re-pays the GDAL split cost. After Phase 2a, the next source read must be the one that persists to Delta. |
| Read source → filter tiles by `rst_boundingbox` ∩ target bbox → clip survivors → merge | Still reads the giant source on every run. The "smart spatial filter" doesn't help; the cost is in the read, not the clip. Retile-and-persist instead. |
| `rst_merge_agg` to a single tile before downstream work | Kills parallelism. Keep tiles separate; only merge if the final output format requires a single raster. |
| `.write.format("gtiff_gdal").save(volume_path)` | `gtiff_gdal` is a **reader**, not a writer. This call does not produce a usable GTiff file. Persist as Delta tables; if you need a GTiff file artifact, extract `rst_asformat("tile", "GTiff")` and write the bytes via a different mechanism (e.g., `dbutils.fs.put` on the binary content). |
| Picking `sizeInMB` "to make a large file manageable" | `sizeInMB` is an MB-bounded read-time split. It does not solve the underlying "read this giant file many times" problem. Use retile-and-persist. |
| `rx.read_raster(...)`, `rx.load_tiff(...)`, `rx.from_path(...)`, or any other invented `rx.*` reader | These do NOT exist. Hallucinated function names. Raster ingestion is always via `spark.read.format(...)` per Phase 2b/2c. Check the `api_names` list from Phase 1b before calling any unfamiliar `rx.*` function. |

### Phase 3: Process

Register functions, then operate on the `tile` column with whatever operations the user's workflow needs. **None of the operations below are required** — pick the ones that fit. Many raster workflows have no clip step at all (global aggregation, reprojection, format conversion, mosaic). Don't assume "clip to a region" is the default just because it's a common example.

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import col, lit
rx.register(spark)

# --- Examples (pick what's needed; not a required sequence) ---

# Metadata extraction (almost always useful for sanity-checking)
meta = rasters.select(
    "source",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("bands"),
    rx.rst_srid("tile").alias("srid"),
    rx.rst_summary("tile").alias("stats"),
)

# Clip by geometry — only when the workload calls for a spatial subset.
# `geom` here is whatever column holds the target polygon (UC GEOMETRY / WKT / WKB).
# If a polygon table only has bbox columns (xmin/xmax/ymin/ymax), build a polygon
# from them first; do not assume a `geom` column exists.
clipped = rasters.select(rx.rst_clip("tile", col("geom")).alias("tile"))

# Reproject to a target CRS (e.g., Web Mercator for tile maps).
reproj = rasters.select(rx.rst_transform("tile", lit(3857)).alias("tile"))

# Vegetation index — NDVI for a multi-band scene (red=1, nir=2 for Sentinel-2).
ndvi = rasters.select(rx.rst_ndvi("tile", lit(1), lit(2)).alias("ndvi"))
```

The right Phase 3 operations depend entirely on the user's workflow. Ask if it's not stated; don't reach for clip-to-polygon by default.

See `references/functions.md` for the full function catalog.

### Phase 4: Persist

Write outputs to Delta in UC. For derived rasters, convert to a canonical format (e.g., COG) before writing.

**Do not assume a hardcoded `main.<schema>.<table>`.** Always confirm the destination with the user:

1. **Propose a catalog/schema** from context. If the user's source data (TIFF, Volume) is under `/Volumes/<catalog>/<schema>/...`, propose writing the output to that same `<catalog>.<schema>.<table_name>`.
2. **Propose a meaningful table name** based on the workload — match the actual operation (e.g., `processed_rasters` for generic transforms, `<index>_h3` for H3-aggregated results, `<source>_reprojected` for CRS transforms). Don't reach for clip/region-specific naming unless the workflow is actually clipping.
3. **Phrase as confirmation**, not open question:
   > *"I'll write the output to `<catalog>.<schema>.<table_name>` (same catalog/schema as your source data). Confirm, or specify a different target."*
4. **Offer to create the schema** (`CREATE SCHEMA IF NOT EXISTS`) if the proposed catalog/schema doesn't exist yet.

```python
output_table = "${catalog}.${schema}.${table_name}"  # confirmed with user

(rasters
    .withColumn("tile_cog", rx.rst_asformat("tile", lit("COG")))
    .write.mode("overwrite").saveAsTable(output_table))
```

Apply the same propose-and-confirm pattern wherever a table name is needed (`retiled_table` in the large-raster pattern, intermediate Delta tables, etc.).

## Pattern: Large raster ingestion (retile-and-persist)

**This is where Phase 2c routes when the size check in 2a flags the source as LARGE** — global scenes, multi-band imagery, or anything where the input is more than ~5–10 GB. The standard Phase 2 → Phase 3 flow (read, operate, write) doesn't scale well to global-coverage rasters because every downstream op re-reads the source. For scenes like VIIRS / Landsat / Sentinel mosaics, **read → retile → persist → operate** is the production pattern.

### Why the simple pattern fails at scale

| Symptom | Cause |
|---|---|
| First operation hangs for many minutes | GDAL has to scan and split the giant file every time |
| Driver OOM on metadata extraction | Catalog enumeration over many splits in a single call |
| Downstream ops re-read the source TIFF | Without persisting, Spark recomputes the split for each transform |
| Long tail on a few executors | `sizeInMB=16` splits are MB-bounded, not pixel-bounded — uneven tile shapes |

### Step-by-step

```python
import math
from pyspark.sql import functions as F
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

tiff_path = "${tiff_path}"
retiled_table = "${catalog}.${schema}.retiled_raster"

# 1. Read the raster — explicit driver. The reader splits the file by `sizeInMB` (default 16)
#    so each row in `raster_df` represents one CHUNK, not the whole file.
raster_df = (
    spark.read.format("gdal")
        .option("driverName", "GTiff")
        .load(tiff_path)
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
with rasterio.open(tiff_path) as src:
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

### Tuning tile size

The auto-derived size targets ~200 total tiles. Override `target_total_tiles` if you have specific needs:

| Trade-off | Increase target tiles | Decrease target tiles |
|---|---|---|
| Parallelism | More tiles, more parallel work | Fewer tiles, less parallel |
| Per-tile work | Smaller tiles, less work each | Larger tiles, more work each |
| File / metadata overhead | More files in Delta, higher overhead | Fewer files, less overhead |
| Memory per tile | Lower (smaller tile fits comfortably) | Higher (large tile may OOM) |

For a cluster with ~4 workers, ~200 tiles gives each worker ~50 tiles — a good default. Tune higher (500+) for very large clusters; tune lower (100) if per-tile ops are expensive.

### Aggregating tiles to H3 (with `[0]` band-index gotcha)

`rst_h3_rastertogridavg` returns an **array-of-arrays**: one array per band, each containing `(cellID, measure)` tuples. For single-band rasters (VIIRS DNB, DTM, single-channel imagery), index `[0]` to pick band 1 before exploding:

```python
h3_resolution = 8  # tune for the workload; ~0.7 km² per cell at res 8

h3_df = (
    tiles
    .withColumn("h3_stats", rx.rst_h3_rastertogridavg("tile", F.lit(h3_resolution)))
    .select(F.explode(F.col("h3_stats")[0]).alias("c"))   # [0] = band 1
    .select(
        F.col("c.cellID").alias("h3_cell"),
        F.col("c.measure").alias("mean_value"),            # rename to match the source semantic (e.g. mean_elevation, mean_radiance, mean_temp)
    )
)
```

For multi-band rasters, loop over `[band_index]` and union/concat the results, or explode each separately with a band-label column.

### When NOT to use this pattern

- Small rasters (< 1 GB) — the persist-to-Delta overhead isn't worth it. Stick to the standard Phase 2–4 flow.
- Single-tile operations (one polygon, one zonal stat) — the metadata-then-clip path is faster.
- Truly streaming workloads — this pattern is batch-oriented; for continuous ingest, you'd structure differently.

## Common Issues

| Issue | Cause / Fix |
|---|---|
| `SHOW FUNCTIONS LIKE 'gbx_rst_*'` returns empty | Init script didn't run — check cluster event log, verify `VOL_DIR` and Volume path |
| `UnsatisfiedLinkError: libgdalalljni.so` | `.so` not copied to `/usr/lib/` — re-check init script |
| `Driver not found` on read | Provide `driverName` option, or use a named reader (`gtiff_gdal`) |
| Out-of-memory on large rasters | Increase `sizeInMB` split, or use `rx.rst_retile` to chunk |
| Serverless cluster fails | GeoBrix requires classic clusters — Serverless not supported |

## Resources

### References
- `references/install.md` — Detailed cluster setup, init script content, troubleshooting
- `references/functions.md` — Full RasterX function reference (metadata, transformations, generators, H3 aggregation)

### External
- Docs: https://databrickslabs.github.io/geobrix/
- GitHub: https://github.com/databrickslabs/geobrix
- Releases (download artifacts): https://github.com/databrickslabs/geobrix/releases
- Native DBSQL spatial (public preview DBR 17.1+): https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions

## Examples

### Example 1: NDVI from Sentinel-2 imagery
User says: *"I have Sentinel-2 tiles in a Volume — compute NDVI and save to Delta"*

Result: Skill scaffolds the read using `gtiff_gdal`, applies `rx.rst_ndvi(tile, red_band, nir_band)`, then writes a Delta table partitioned by date.

### Example 2: Zonal statistics over administrative boundaries
User says: *"Compute mean elevation per county from a DTM raster"*

Result: Join raster tiles with county polygons via `rx.rst_clip`, then aggregate using `rx.rst_avg` per polygon. Output is a Delta table keyed by county_id.

### Example 3: Raster reprojection
User says: *"My rasters are in mixed CRSes — normalize them to Web Mercator before joining"*

Result: Use `rx.rst_srid("tile")` to inspect the source CRS, then `rx.rst_transform("tile", lit(3857))` to reproject. Write to Delta keyed by source CRS for auditability.

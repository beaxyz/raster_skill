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

    # AUTHORITATIVE signal — the Python API surface you actually call. Prefix-independent.
    # Use ONLY names that appear here for any rx.<name>(...) call.
    # Do not invent names (no read_raster, no load_tiff, etc.)
    api_names = sorted(name for name in dir(rx) if not name.startswith("_"))
    rst_fns = [n for n in api_names if n.startswith("rst_")]
    print(f"✅ GeoBrix installed: {len(rst_fns)} rst_* functions on the Python API.")

    # SQL registration check — the SQL prefix varies by version (`rst_*` or `rst_*`),
    # so match EITHER with a contains-pattern. Do NOT hardcode one prefix: a wrong guess
    # returns 0 silently and looks like "installed but empty".
    n_sql = spark.sql("SHOW FUNCTIONS LIKE '*rst_*'").count()
    print(f"   SQL functions registered (matching *rst_*): {n_sql}")

    print("\nrx.<...> API (use only these names):")
    print(", ".join(api_names))
except Exception as e:
    print(f"❌ GeoBrix not installed or misconfigured: {e}")
```

**If `rst_fns` is non-empty** → GeoBrix is installed, proceed to Phase 2. (The `dir(rx)` list is the source of truth; the SQL count is a secondary confirmation that registration also exposed the functions to Spark SQL.)

**Determine the SQL prefix for THIS install before calling any function in SQL.** Don't assume either `gbx_rst_` or `rst_` — it varies by version. If you need the SQL names, print them: `spark.sql("SHOW FUNCTIONS LIKE '*rst_*'").show(truncate=False)` — and use exactly what it prints. In Python (the form this skill uses throughout), it's always `rx.rst_*`.

**Hard rule on the API surface:**
- `rx` exposes only the names printed above (typically `register` + `rst_<...>` functions).
- **There is NO `rx.read_raster`, `rx.load_tiff`, `rx.from_path`, or any other "read" helper on the `rx` module.** Raster ingestion is always via `spark.read.format("gtiff_gdal" | "gdal").load(path)` (Phase 2b) or via the prescribed retile-and-persist pattern (Phase 2c). If you find yourself reaching for an `rx.read_*` or `rx.load_*` function, stop — it doesn't exist.
- **For function details (signature, purpose, category) → read `references/functions.md`.** It's the curated catalog of every `rst_*` function organized by category (metadata, transformations, generators, H3 aggregation, etc.).
- **DO NOT run `DESCRIBE FUNCTION EXTENDED <name>` on each function** as a way to learn the API (and note the SQL name's prefix varies by version — see Phase 1b). It's slow (one SQL call per function), the output is Spark-formatted (not user-friendly), and the same information already lives in `references/functions.md`. The only valid use of `DESCRIBE FUNCTION EXTENDED` is debugging a specific signature mismatch at runtime.
- If you're unsure whether a function exists or what it does, in order: (1) check the printed `api_names` list above, (2) grep `references/functions.md`, (3) if both miss, the function doesn't exist — don't invent it.

**If the import or SQL check fails** on a classic cluster → the cluster needs bootstrap. See `references/install.md` for the full procedure (download artifacts → upload to UC Volume → configure init script + library on a classic DBR 17.1+ cluster).

Before running any install step, **collect parameters from the user** (Volume path, cluster ID, GeoBrix version) — `install.md` lists them up-front and the snippets won't work with placeholders. Once installed, re-run the check above.

### Phase 2: STOP. Run the size check before anything else.

**🛑 BEFORE writing any code that touches the source raster, you (Claude / Genie Code) must emit and run the `dbutils.fs.ls` size check below. This is not advisory. It is the first code cell of any GeoBrix raster session after Phase 1.**

Do not estimate the source size from the filename, the dataset name (e.g. "VIIRS"), prior conversation history, or general knowledge of typical satellite product sizes. Real numbers only.

#### What counts as "touching the source" — all of these are forbidden before Phase 2a

The rule is about **timing, not syntax**. Do not emit any of these against the source path until Phase 2a has run and its output is in the conversation:

- `spark.read.format("gtiff_gdal" | "gdal" | ...).load(source_path)` — the standard reader path
- `spark.sql("SELECT * FROM rst_maketiles('...')")` or any **SQL TVF** that takes the file path
- `rx.rst_fromfile(source_path)` / `rst_fromfile(source_path)` — direct constructor
- `rx.rst_fromcontent(...)` / `rst_fromcontent(...)` reading from `binaryFile`-loaded bytes of the source
- Any other GeoBrix function that takes a file path / URI to the source
- Any `dbutils.fs.head` / `binaryFile` / `spark.read.format("binaryFile")` of the source (these read bytes too)

The only operation allowed against the source path before Phase 2a runs is **`dbutils.fs.ls`** itself (it's metadata-only, no byte read).

#### Phase 2a. Size check (MANDATORY first action)

```python
items = dbutils.fs.ls(source_path)  # works on single file or directory; metadata only, no byte read

# --- Detect format FIRST. The byte-based size check below is a SINGLE-GRID (GeoTIFF)
#     heuristic: it assumes bytes ≈ pixels ≈ how hard this is to process. That assumption
#     is FALSE for NetCDF/GRIB/HDF (variables-as-subdatasets, multidimensional:
#     vars × time × level × grid), so routing MUST branch on format before measuring size.
def _ext(path):
    leaf = path.rstrip("/").rsplit("/", 1)[-1].lower()
    return leaf.rsplit(".", 1)[-1] if "." in leaf else ""

leaf = source_path.rstrip("/").rsplit("/", 1)[-1]
exts = {_ext(source_path)} if "." in leaf else {_ext(i.path) for i in items}  # file → its ext; dir → contents' exts
exts.discard("")
SUBDATASET_EXTS = {"nc", "nc4", "grib", "grib2", "grb", "hdf", "hdf5", "h5"}
is_subdataset_fmt = bool(exts & SUBDATASET_EXTS)

total_bytes = sum(item.size for item in items)
total_gb = total_bytes / (1024**3)
n_files = len(items)
print(f"{n_files} file(s), total {total_gb:.2f} GB; format(s): {sorted(exts) or ['unknown']}")

if is_subdataset_fmt:
    # NetCDF/GRIB/HDF — bytes ≠ spatial size. Do NOT byte-route; do NOT spatially retile the raw file.
    routing = "SUBDATASET"
    print("\n*** ROUTING: SUBDATASET (NetCDF/GRIB/HDF) ***")
    print("Byte-based LARGE/SMALL does NOT apply — bigness is logical (vars × time × level),")
    print("not GB on disk. GeoBrix officially supports GeoTIFF only; NetCDF/GRIB are best-effort.")
    print("Go to Phase 2d: enumerate subdatasets -> select variable/time -> subset ->")
    print("write GeoTIFF/COG -> re-run THIS size check on that GeoTIFF output.")
else:
    # Single-grid rasters (GeoTIFF/COG, JP2, IMG): bytes ≈ pixels, the heuristic holds.
    if n_files == 1:
        print(f"Single file: {total_gb:.2f} GB")
        avg_mb = total_gb * 1024
    else:
        avg_mb = total_bytes / n_files / (1024**2)
        print(f"avg {avg_mb:.1f} MB/file")

    if total_gb > 5:
        routing = "LARGE"
        print("\n*** ROUTING: LARGE ***")
        print("MUST use the retile-and-persist pattern in references/examples/large-raster-retile.md.")
        print("MUST NOT write any spark.read.format(...).load(source_path) call against the source.")
    elif n_files > 100 and avg_mb > 500:
        routing = "LARGE"
        print("\n*** ROUTING: LARGE (many large files) ***")
        print("MUST use the retile-and-persist pattern.")
    else:
        routing = "SMALL/MEDIUM"
        print("\n*** ROUTING: SMALL/MEDIUM ***")
        print("Phase 2b standard read is appropriate.")

print(f"\nRouting decision: {routing}")
```

**After this cell runs, look at its output. The routing decision drives everything downstream.** Note the detected format(s): if it prints `['unknown']` (e.g. extension-less files), inspect the source manually before assuming the single-grid path — a NetCDF/GRIB file without an extension will otherwise be byte-routed incorrectly.

#### Hard rules — non-negotiable, no creative interpretation

1. **If routing is LARGE, you MUST jump to Phase 2c → retile-and-persist.** Skip Phase 2b entirely. Do not write `spark.read.format("gtiff_gdal").load(huge_source)`. Do not write a "smarter" optimization (see anti-patterns below). The retile-and-persist pattern is the prescribed path; there is no approved alternative.

2. **No "optimizations" that still read the source directly.** Variations like:
   - "Read with `sizeInMB` splitting, then filter tiles by `rst_boundingbox` intersection before clipping"
   - "Read once, clip to bbox, merge, then process"
   - "Read with a smaller `sizeInMB` to make it more manageable"

   ...are all the same anti-pattern. They all read the giant source on every operation. The retile-and-persist pattern is the only approved approach for LARGE sources.

3. **No reasoning around the size check.** Don't write "the file is probably small enough" or "since the output (clip to one city) will be small, we can skip retile-and-persist." The skill enforces routing based on **input** size, not anticipated output size.

4. **Thresholds are minimum-bars, not preferences.** When in doubt between standard read and retile-and-persist, choose retile-and-persist.

5. **You must surface the routing decision to the human.** After Phase 2a runs, your next message should state: "Phase 2a routed this as [LARGE / SMALL-MEDIUM / SUBDATASET]. Proceeding with [retile-and-persist / standard read / NetCDF subset-to-GeoTIFF]." Do not silently transition.

6. **If routing is SUBDATASET (NetCDF/GRIB/HDF), go to Phase 2d.** Do NOT apply the GB thresholds, do NOT spatially retile the raw file, and do NOT read it through Phase 2b/2c as if it were a single grid. The byte-based gate is a GeoTIFF heuristic and is meaningless here — the file's "size" is its logical shape (variables × timesteps × levels), which `dbutils.fs.ls` cannot see.

#### Phase 2b. Standard read (SMALL/MEDIUM routing only — single-grid formats)

**Only enter this section if Phase 2a printed `ROUTING: SMALL/MEDIUM`.** That routing is reachable only for single-grid formats (GeoTIFF/COG, JP2, IMG). For LARGE jump to Phase 2c; for SUBDATASET (NetCDF/GRIB/HDF) jump to Phase 2d.

Pick the reader based on the source extension:

```python
ext = source_path.lower().rsplit(".", 1)[-1]

if ext in ("tif", "tiff"):
    rasters = spark.read.format("gtiff_gdal").load(source_path)   # GeoTIFF / COG
else:
    # Other single-grid GDAL formats — auto-detect, or set driverName explicitly
    # (e.g. "JP2OpenJPEG" for .jp2, "HFA" for .img).
    # NOTE: NetCDF/GRIB are NOT read here — they route to Phase 2d.
    rasters = spark.read.format("gdal").load(source_path)

# Optional: split per-file at a target size. Add only if files are several hundred MB+.
# .option("sizeInMB", "32")
```

The output has a `tile` column ready for RasterX functions. Proceed to Phase 3.

#### Phase 2c. LARGE routing → retile-and-persist

If Phase 2a printed `ROUTING: LARGE`, skip Phase 2b and follow the **retile-and-persist pattern** in `references/examples/large-raster-retile.md` — the path for LARGE **single-grid** rasters (GeoTIFF/COG, `.jp2`, `.img`). It handles read + inspect + auto-sized retile + Delta persist, and feeds downstream phases from the persisted table. **NetCDF/GRIB are out of scope for that pattern** — they expose variables as subdatasets and their CRS may need checking (sometimes inferred from CF metadata, sometimes absent), so their read + metadata steps differ before any retile.

#### Phase 2d. SUBDATASET routing → NetCDF / GRIB / HDF

If Phase 2a printed `ROUTING: SUBDATASET`, the byte-based size gate does **not** apply and you must **not** feed the raw file into Phase 2b/2c. These formats expose multiple **variables as subdatasets** and are often multidimensional (`time × level × lat × lon`); their CRS may be inferred from CF metadata or absent (check `rst_srid`, don't assume).

> ⚠️ **GeoBrix officially supports GeoTIFF only** (per its readers list — see Resources). NetCDF/GRIB are best-effort via the generic GDAL driver, with no guarantees. The robust path is to reduce them to GeoTIFFs and then use the supported single-grid flow. Because support is best-effort, falling back to a NetCDF-native tool (xarray / rioxarray) for the *extraction* step is more legitimate here than it would be for GeoTIFF — but still surface the choice to the user per the substitution policy.

**Strategy (don't byte-route, don't spatially retile the raw file):**

1. **Enumerate subdatasets / variables** — `rx.rst_subdatasets("tile")` (or `gdalinfo` / xarray) to see the variables and their dimensions. The "size" that matters is the *logical shape* — how many variables × timesteps × levels, at what per-slice grid — not GB on disk.
2. **Select** the variable(s) and time/level slice(s) you actually need; drop the rest. This is usually where the real data-volume reduction happens.
3. **Subset / rechunk** the selected slices. Two viable outputs (the worked example uses the second): convert to GeoTIFF/COG and re-enter the single-grid flow, **or** rechunk to smaller `.nc` files with xarray and read them directly with `driverName="netCDF"`.
4. **Analyse** — check `rx.rst_srid("tile")` first (GDAL often infers the CRS from CF metadata; if it's genuinely `0`, ask the user for the EPSG — don't hardcode). Align to your boundary's CRS with `rx.rst_transform` before clip/H3. Bands map to **timesteps**, not spectral bands.

**Worked example: `references/examples/netcdf-ingest.md`** — full two-scenario flow (one big `.nc` → rechunk by a variable → analyse chunked files), with the CRS check/align step, hourly-band explode, clip-to-city, and H3 time-series.

#### Anti-patterns — DO NOT EMIT THESE for LARGE sources

| Anti-pattern | Why it's wrong |
|---|---|
| `spark.read.format("gtiff_gdal").load(huge_file)` | Reads the giant file, often multiple times. The MB-bounded split is not a substitute for retile-and-persist. |
| `SELECT * FROM rst_maketiles('{huge_file}', ...)` as the ingest step | `rst_maketiles` is a **generator** (build tiles from an extent + grid definition), not a file-ingest function. Using it to ingest a TIFF is either a misuse or a workaround for the size-check gate. Use the prescribed read pattern (small/medium) or retile-and-persist (large). |
| `rst_fromfile(huge_file)` as the entry point | Same family as the SQL TVF above — a "direct constructor" that bypasses the size-check gate. Allowed only after Phase 2a routes the source as SMALL/MEDIUM. |
| Read source twice (e.g. once for metadata, once for clipping) | Every downstream op re-pays the GDAL split cost. After Phase 2a, the next source read must be the one that persists to Delta. |
| Read source → filter tiles by `rst_boundingbox` ∩ target bbox → clip survivors → merge | Still reads the giant source on every run. The "smart spatial filter" doesn't help; the cost is in the read, not the clip. Retile-and-persist instead. |
| `rst_merge_agg` to a single tile before downstream work | Kills parallelism. Keep tiles separate; only merge if the final output format requires a single raster. |
| `.write.format("gtiff_gdal").save(volume_path)` | `gtiff_gdal` is a **reader**, not a writer. This call does not produce a usable GTiff file. Persist as Delta tables; if you need a GTiff file artifact, extract `rst_asformat("tile", "GTiff")` and write the bytes via a different mechanism (e.g., `dbutils.fs.put` on the binary content). |
| Picking `sizeInMB` "to make a large file manageable" | `sizeInMB` is an MB-bounded read-time split. It does not solve the underlying "read this giant file many times" problem. Use retile-and-persist. |
| `rx.read_raster(...)`, `rx.load_tiff(...)`, `rx.from_path(...)`, or any other invented `rx.*` reader | These do NOT exist. Hallucinated function names. Raster ingestion is always via `spark.read.format(...)` per Phase 2b/2c. Check the `api_names` list from Phase 1b before calling any unfamiliar `rx.*` function. |

### Phase 3: Process

**Phase 3 is format-agnostic.** Phases 2b/2c/2d all converge on a `tile` column; from here the analytics are the same whether the source was a GeoTIFF, a retiled large scene, or a NetCDF reduced to GeoTIFFs. Register functions, then operate on the `tile` column with whatever operations the user's workflow needs. **None of the operations below are required** — pick the ones that fit. Many raster workflows have no clip step at all (global aggregation, reprojection, format conversion, mosaic). Don't assume "clip to a region" is the default just because it's a common example.

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

# H3 — tessellate/aggregate pixels to hex cells.
# Unpacking depends on what a "band" is (single-band [0] vs spectral vs NetCDF timesteps).
# See references/examples/h3-examples.md for the example types + function list.
```

The snippets above are a quick taste. The worked, format-agnostic analytics live in dedicated docs (they run on the `tile` column from any ingestion route):

- `references/examples/raster-analytics.md` — per-band stats (incl. multi-temporal), clip to a boundary polygon, zonal stats
- `references/examples/h3-examples.md` — H3 tessellate / aggregate / time-series, the band-semantics matrix, function list

The right Phase 3 operations depend entirely on the user's workflow. Ask if it's not stated; don't reach for clip-to-polygon by default. See `references/functions.md` for the full function catalog.

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

## Common Issues

| Issue | Cause / Fix |
|---|---|
| `SHOW FUNCTIONS LIKE '*rst_*'` returns empty (but `dir(rx)` shows `rst_*`) | Registration didn't expose SQL functions — re-run `rx.register(spark)`; if `dir(rx)` is also empty, the init script didn't run (check cluster event log, verify `VOL_DIR` and Volume path) |
| `UnsatisfiedLinkError: libgdalalljni.so` | `.so` not copied to `/usr/lib/` — re-check init script |
| `Driver not found` on read | Provide `driverName` option, or use a named reader (`gtiff_gdal`) |
| Out-of-memory on large rasters | Increase `sizeInMB` split, or use `rx.rst_retile` to chunk |
| Serverless cluster fails | GeoBrix requires classic clusters — Serverless not supported |

## Resources

### References
- `references/install.md` — Detailed cluster setup, init script content, troubleshooting
- `references/functions.md` — Full RasterX function reference (metadata, transformations, generators, H3 aggregation)
- `references/examples/large-raster-retile.md` — Large **single-grid** raster (LARGE routing) retile-and-persist pattern for GeoTIFF/COG, `.jp2`, `.img`: read → inspect true dimensions → auto-sized retile → Delta persist → H3 aggregation (NetCDF/GRIB out of scope)
- `references/examples/netcdf-ingest.md` — NetCDF/GRIB (SUBDATASET routing) two-scenario flow: rechunk one big `.nc` by a variable (xarray), then analyse chunked files with GeoBrix — CRS check/align, hourly-band explode, clip-to-city, H3 time-series
- `references/examples/raster-analytics.md` — **Phase 3, format-agnostic** analytics on a `tile` column: summary metadata, per-band stats (incl. multi-temporal `band_index → timestamp`), clip to a boundary polygon, zonal stats
- `references/examples/h3-examples.md` — **Phase 3, format-agnostic** H3 example types on a `tile` column: tessellate, aggregate per cell, multi-temporal time-series, coarse-grid pre-retile, region filtering/rendering; band-semantics matrix + function list

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

---
name: databricks-geobrix-raster
description: Ingest and process raster geospatial data at scale on Databricks with GeoBrix RasterX — the Spark-native, Unity-Catalog-integrated successor to Mosaic. Use this skill for two things: (1) INGESTING raster files into a distributed `tile` column via the GDAL/rasterio reader, and (2) PROCESSING rasters that are too large for one node or span many scenes. Ingestion is first-class for GeoTIFF/COG (.tif/.tiff) and best-effort for NetCDF (.nc) and GRIB (.grib/.grib2). A single small raster does NOT need GeoBrix — a library like rasterio does an NDVI, clip, or reproject in a few lines on one node; GeoBrix earns its place when the work is distributed (many scenes, or one scene too big for a node) or must live inside a Spark/UC pipeline. Triggers on terms like raster, GeoTIFF, .tif, .tiff, COG, satellite imagery (Sentinel, Landsat — GeoTIFF), weather/climate grids (GRIB/NetCDF), DEM/elevation, land cover, nighttime lights, read raster, ingest raster, raster to Delta, NDVI, zonal stats, raster to H3, clip/reproject/mosaic raster, GeoBrix, RasterX, GDAL raster reader, spatial raster.
---

# Databricks GeoBrix Raster

Process raster geospatial data on Databricks using the GeoBrix library — the next-generation Databricks Labs replacement for Mosaic. Covers GeoTIFF ingestion, raster transformations (clip, reproject, mosaic), spectral indices (NDVI), H3 grid aggregation, and zonal statistics.

## When to Use This Skill

GeoBrix RasterX is a **distributed** raster engine. It earns its place in two situations:
**ingesting** raster files into Spark, and **processing** rasters at a scale where a single
node won't do. Recognize the task type first — it changes whether GeoBrix is even the right
tool. Users typically describe data in domain terms (mission, phenomenon); recognize the
pattern even when GeoBrix isn't mentioned by name.

### A. Ingestion triggers — GeoBrix + the GDAL/rasterio reader IS the tool

These are "read this raster file into a distributed `tile` column" tasks. Ingestion support
is **tiered by how well the format is actually tested in GeoBrix** — not by what GDAL claims
its 150+ drivers can do:

| Priority | Formats | Support level | Typical sources |
|---|---|---|---|
| **P0 — First-class** | GeoTIFF, COG (`.tif`/`.tiff`) | Tested & optimized; the documented happy path | Sentinel-2, Landsat, SRTM/DEM, land cover, nighttime lights — nearly all ship as GeoTIFF |
| **P1 — Best-effort (tested)** | NetCDF (`.nc`), GRIB/GRIB2 (`.grb`/`.grib2`) | Real test coverage; use SUBDATASET routing (Phase 2d) | Weather/climate grids (HRRR, ERA5), oceanographic, multi-dimensional science data |

If a file's format is **not** in this table, do not assume it works — see
`references/todo.md` (the ingestion backlog) before promising support.

Trigger phrases for ingestion, even without "GeoBrix":
- "I have a TIFF / GeoTIFF / COG at..." / "read this satellite image into Spark"
- "Ingest these rasters to Delta" / "catalog a directory of .tif files"
- "Sentinel-2 / Landsat [anything]" (→ GeoTIFF, P0)
- "HRRR / ERA5 / weather grid / climate NetCDF" (→ GRIB or NetCDF, P1)

### B. Processing triggers — GeoBrix only when the work is DISTRIBUTED

These operate on the `tile` column *after* ingestion (clip, reproject, mosaic, NDVI, H3
aggregation, zonal stats). **None of them require GeoBrix on their own.** A single small
raster (one scene, fits in memory) does NDVI, a clip, or a reproject in a few lines of
rasterio/numpy on one node — GeoBrix would be slower and heavier for that.

Reach for GeoBrix for a processing task **only when** it is genuinely distributed:
- **many scenes** (a directory of tiles, a temporal stack), or
- **one scene too big for a single node**, or
- the output must live **inside a Spark / Unity Catalog pipeline** (Delta tables, governance).

This gate is stated here but **applied in Phase 2a Stage 1**, once `dbutils.fs.ls` gives real
format + size numbers — don't pre-judge from the filename or dataset name. Stage 1 is where you
weigh these signals and, if the source looks like a single small scene, ask the user before
committing to GeoBrix. See `references/functions.md` for the processing functions available
on the `tile` column (spectral indices, clip, reproject, mosaic, H3/zonal aggregation).

> A worked "patterns for `rst_*` processing functions" section is deferred until needed —
> this skill's current focus is **ingestion by format**. The processing functions are
> catalogued in `references/functions.md`; add worked patterns there when the need arises.

### C. Dataset / domain phrasing → resolves to a format above

Users describe data by mission, not format. Map it, then apply A (ingest) then B (process):
- "Sentinel-2 / Landsat / land cover / nighttime lights [anything]" → GeoTIFF (P0)
- "SRTM / DEM / elevation / terrain" → GeoTIFF (P0)
- "HRRR / ERA5 / weather / climate grid / oceanographic" → GRIB or NetCDF (P1)

Do NOT use this skill for:
- Pure vector geospatial work (points, lines, polygons without rasters) — use native DBSQL `ST_` functions (covered in `databricks-dbsql`)
- H3 indexing of point/polygon data alone (no raster involved) — use native DBSQL H3 functions
- **A single small raster where the ask is one NDVI / clip / reproject** — rasterio on one node is the right tool; GeoBrix adds overhead with no scale benefit

## Background

GeoBrix is the successor to DBLabs Mosaic, modernized for the Data Intelligence Platform. It exposes three packages — but only **RasterX** is meaningfully the right tool for new work:

- **RasterX** — raster processing (the focus of this skill).
- **GridX** — discrete global grid indexing. For fresh **H3 work on point/polygon data**, use **native DBSQL `H3_*` functions** (covered in the `databricks-dbsql` skill). GridX is the right tool only for **BNG (British National Grid)** or **migrating Mosaic grid code**.
- **VectorX** — *not the right tool for general vector work.* It contains a single migration helper for converting legacy Mosaic geometries to native UC `GEOMETRY`/`GEOGRAPHY` types. For all other vector operations (`ST_Intersects`, buffers, spatial joins, geometry construction), use **native DBSQL `ST_` functions** (also covered in `databricks-dbsql`).

GeoBrix is Beta and community-maintained (AS-IS, no SLA). It ships two **execution tiers** — **Lightweight** (`pyrx`, Serverless + classic) and **Heavyweight** (`rasterx`, classic x86 only). See [execution tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers/) and [databrickslabs.github.io/geobrix](https://databrickslabs.github.io/geobrix/).

## Execution tier selection

**Canonical rulebook** for tier, compute, readers, and install routing. Apply this to every raster task — before install, in Phase 1, and when recommending (a)/(b) under Substitution policy below.

Before install, pick **tier** then **compute**. Source: [Choosing an Execution Tier](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers/).

**Defaults (in order):**
1. **Tier = Lightweight** (`pyrx`) unless a heavy-only surface applies
2. **Compute = Serverless** for Lightweight (classic only when Serverless unavailable or undetectable)

| | Lightweight (`pyrx`) | Heavyweight (`rasterx`) |
|---|---|---|
| Install | [`1-install/install-light.md`](references/1-install/install-light.md) — `%pip [light]` wheel | [`1-install/install-heavy.md`](references/1-install/install-heavy.md) — JAR + init script + WHL |
| Compute | Serverless (preferred), classic shared/ARM/dedicated | Classic **x86** only |
| Bootstrap | `register(spark)` for `*_gbx` I/O, then `pyrx.functions.register(spark)` for `rst_*` | `rasterx.functions.register(spark)` only (JAR registers readers) |
| GeoTIFF reader | `gtiff_gbx` | `gtiff_gdal` |
| Generic raster reader | `raster_gbx` | `gdal` |

**Route to Heavyweight when ANY apply:** OGR readers (`*_ogr`), exotic `gdal` driver options, PMTiles writer, `conforming` GridX/VectorX triangulation, existing JAR+init script to reuse, or `pyrx` unavailable in the installed release.

**Route to Lightweight** for typical RasterX work this skill covers when none of the above apply: GeoTIFF/COG read, metadata, `rst_*` analytics (stats, clip, reproject, NDVI, H3, zonal stats), and SMALL/MEDIUM size routing.

**Compute routing:**

| Tier | Default compute | Fallback |
|---|---|---|
| Light | Serverless + `1-install/install-light.md` | Classic + light if Serverless not enabled / not in Connect dropdown |
| Heavy | Classic x86 + heavy install | On Serverless → STOP, switch to classic x86 |

If Lightweight task and user is on classic: recommend Serverless via Connect first; proceed on classic only as fallback.

## Substitution policy — GeoBrix is the default. STOP and ASK before any fallback.

**Once §A/§B confirm the task fits GeoBrix** (distributed — many scenes, one scene too big for a node, or must live in a Spark/UC pipeline), **use GeoBrix — don't fall back to another library (rasterio, gdal CLI, geopandas) just because GeoBrix isn't installed yet.** Falling back there bypasses the entire reason this skill exists (Spark-native distributed processing, UC integration, scale beyond one node) — the fix for "not installed" is to install it (below), not to silently swap in rasterio. (For a task that §B routes to a single-node library in the first place, this policy doesn't apply — that's the right call, not a "fallback.")

**If GeoBrix is not installed on the cluster:**

1. **Tell the user explicitly:** "GeoBrix isn't installed on this cluster."
2. **Run the Phase 2a Stage 1 size check FIRST** (`dbutils.fs.ls` — metadata-only, no byte read, no GeoBrix needed), then **apply Execution tier selection + the §B scale gate** to the real numbers. Summarize in **one sentence**: whether the source even clears the §B gate, and — if GeoBrix is warranted — the recommended tier/compute (Light vs Heavy, Serverless vs classic). If it looks like a single small scene, say so: the honest recommendation there is a single-node library, not an install. If format or compute is ambiguous, state your assumption and what would change it. **Do not install or run processing code based on this alone.**
3. **STOP. Do not proceed with any code execution.** Present that one-line recommendation, then ask and wait for the user's answer. **Lead with the option the gate points to:**
   - **Gate passed** (many scenes / one scene too big for a node / must live in a Spark-UC pipeline) → lead with install:
     > *"GeoBrix isn't installed. Based on your task, I'd recommend **(a) Lightweight** [one-line reason from Execution tier selection] — or **(b) Heavyweight** if [heavy-only condition from that section]. I can also **(c) use a non-GeoBrix fallback for this single task only** if you prefer. Which do you want?"*
   - **Gate not met** (single small scene, one-off op) → lead with fallback, frame install as "only if scaling":
     > *"GeoBrix isn't installed, and this is one small raster — GeoBrix is likely overkill for it. I'd **(c) use rasterio on a single node** for this. If you expect many scenes / very large scenes / a Spark-UC pipeline, I can instead install **(a) Lightweight** or **(b) Heavyweight**. Which do you want?"*
4. **Do NOT infer consent.** A prompt that mentions "exploratory" or "one-off" or "just for this notebook" is not consent. The user asking the original question is not consent. Your tier recommendation is not consent. Only an explicit choice — (a), (b), or (c) — is consent.
5. **If — and only if — the user explicitly picks (c)**, proceed with rasterio (or appropriate alternative). Always flag it, matching the gate outcome:
   - Gate had **passed** (they declined a warranted install) → *"Using rasterio for this one task per your confirmation. GeoBrix is the right tool for multi-file / large-raster / pipeline work — install when ready."*
   - Gate was **not met** (single small scene) → *"Using rasterio on a single node — the right tool for one small raster. Install GeoBrix if this grows to many scenes, very large scenes, or a Spark-UC pipeline."*
6. **If the user picks (a)**, walk through `references/1-install/install-light.md` from the parameter-collection step onwards (Serverless preferred per Execution tier selection; classic fallback if Serverless unavailable).
7. **If the user picks (b)**, walk through `references/1-install/install-heavy.md` from the parameter-collection step onwards.

**Hard rules:**
- Never decide "all fallback conditions are met" on your own and proceed. That's the LLM inferring consent. The skill explicitly forbids it.
- **Recommendations are encouraged; auto-selection is not.** Use Execution tier selection for the recommendation; the user picks (a), (b), or (c).
- Never write code that uses rasterio / gdal / geopandas for **raster processing** (ingest, clip, reproject, mosaic, NDVI, zonal stats) until the user has confirmed (c) in this conversation. This does not cover the two non-processing spots where rasterio is the standard tool and GeoBrix still does all the processing — the metadata-only header read (Phase 2c, see `references/2-ingest/2c-large-raster-retile.md` Option A) and final-step rendering (Phase 5); use them freely, stating which library does what.
- Never frame the fallback as "since this is a one-off, I'll use rasterio." Frame it as a choice: "Want me to install GeoBrix or fall back?"
- Honor the user's explicit (a) or (b) even when it differs from your recommendation, unless Execution tier selection makes it impossible (e.g. Heavy on Serverless → explain and ask them to switch compute or pick Light).

The user invoked this skill because they want GeoBrix. Respect that until they explicitly say otherwise.

## Prerequisites

**Lightweight (default):**
- DBR **17.3 LTS or 18 LTS**; Python 3.12 (Serverless environment **5+** on Serverless)
- UC Volume for WHL staging (no JAR/init script)
- Setup: [`references/1-install/install-light.md`](references/1-install/install-light.md)

**Heavyweight:**
- DBR **17.1+** on classic **x86**
- UC Volume for JAR, `.so`, init script, and WHL
- Cluster admin permissions (`CAN MANAGE`) for init scripts and libraries
- Setup: [`references/1-install/install-heavy.md`](references/1-install/install-heavy.md)

## Workflow

### Phase 1: Pick tier + compute, verify GeoBrix is installed

**1a. Tier selection** — see **Execution tier selection** above. Default: Lightweight on Serverless.

**1b. Detect compute + Serverless-first recommendation**

```python
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
COMPUTE = "serverless" if not cluster_id else "classic"
print(f"Compute: {COMPUTE}")
```

- **Lightweight + classic:** recommend switching to **Serverless** via Connect (unless Serverless not listed). Proceed on classic only as fallback.
- **Heavyweight + Serverless:** STOP — *"This task needs Heavyweight. Switch to a classic x86 cluster via Connect."*
- **SQL Editor:** switch to a notebook with Serverless or classic compute attached.

**1c. Install routing (if not yet installed)**

| Compute | Tier | Install doc |
|---|---|---|
| Serverless | light | [`references/1-install/install-light.md`](references/1-install/install-light.md) |
| Serverless | heavy | STOP → classic x86 |
| Classic | light | [`references/1-install/install-light.md`](references/1-install/install-light.md) (Serverless preferred if available) |
| Classic | heavy | [`references/1-install/install-heavy.md`](references/1-install/install-heavy.md) |
| Any | light, `pyrx` missing | Heavyweight on classic or upgrade GeoBrix |

Before install, **collect `volume_path`** (propose-then-confirm) — both install docs list parameters.

**1d. Verify GeoBrix + inspect API surface**

Try Lightweight first, then Heavyweight if `pyrx` import fails. Lightweight: `register(spark)` then `rx.register(spark)`. Heavyweight: `rx.register(spark)` only — readers come from the cluster JAR.

On clusters with **both** tiers installed, this block selects Light when `pyrx` is importable. If the cluster is heavy-only (JAR + init script, no `[light]` wheel), the heavy branch runs automatically.

```python
TIER = None
rx = None

try:
    from databricks.labs.gbx.ds.register import register
    from databricks.labs.gbx.pyrx import functions as rx
    register(spark)
    rx.register(spark)
    TIER = "light"
    GEOTIFF_READER = "gtiff_gbx"
    GENERIC_READER = "raster_gbx"
except ImportError:
    try:
        from databricks.labs.gbx.rasterx import functions as rx
        rx.register(spark)
        TIER = "heavy"
        GEOTIFF_READER = "gtiff_gdal"
        GENERIC_READER = "gdal"
    except Exception as e:
        print(f"❌ GeoBrix not installed: {e}")

if rx is not None:
    api_names = sorted(name for name in dir(rx) if not name.startswith("_"))
    rst_fns = [n for n in api_names if n.startswith("rst_")]
    n_sql = spark.sql("SHOW FUNCTIONS LIKE '*rst_*'").count()
    print(f"✅ GeoBrix {TIER}: {len(rst_fns)} rst_* functions")
    print(f"   Readers: {GEOTIFF_READER}, {GENERIC_READER}")
    print(f"   SQL functions (matching *rst_*): {n_sql}")
    print("\nrx API:", ", ".join(api_names))
```

**If both imports fail** → install per 1c. Lightweight: `1-install/install-light.md`. Heavyweight: `1-install/install-heavy.md`.

**Hard rule on the API surface:**
- `rx` exposes only the names printed above (typically `register` + `rst_<...>` functions).
- **There is NO `rx.read_raster`, `rx.load_tiff`, `rx.from_path`, or any other "read" helper on the `rx` module.** Raster ingestion is via `spark.read.format(...)` (Phase 2b/2c). On Lightweight use `gtiff_gbx`/`raster_gbx`; on Heavyweight use `gtiff_gdal`/`gdal`.
- **For function details → read `references/functions.md`.**
- **DO NOT run `DESCRIBE FUNCTION EXTENDED` on each function** to learn the API.
- If unsure whether a function exists: (1) check `api_names`, (2) grep `references/functions.md`, (3) don't invent it.

**1e. Install failure troubleshooting**
- Light on Serverless: re-check PEP 508 `%pip` string and Serverless env 5+
- `pyrx` ImportError after install: release may lack Lightweight → heavy on classic
- Heavy: init script / JAR issues → see `1-install/install-heavy.md` troubleshooting table

### Phase 2: STOP. Run the size check before anything else.

**🛑 BEFORE writing any code that touches the source raster, you (Claude / Genie Code) must emit and run the `dbutils.fs.ls` size check below (Phase 2a Stage 1). This is not advisory. It is the first code cell of any GeoBrix raster session after Phase 1.**

Do not estimate the source size from the filename, the dataset name (e.g. "VIIRS"), prior conversation history, or general knowledge of typical satellite product sizes. Real numbers only.

#### What counts as "touching the source" — all of these are forbidden before Phase 2a

The rule is about **timing, not syntax**. Do not emit any of these against the source path until Phase 2a (Stage 1, and Stage 2 where it applies) has run and its output is in the conversation:

- `spark.read.format("gtiff_gdal" | "gdal" | ...).load(source_path)` — the standard reader path
- `spark.sql("SELECT * FROM rst_maketiles('...')")` or any **SQL TVF** that takes the file path
- `rx.rst_fromfile(source_path)` / `rst_fromfile(source_path)` — direct constructor
- `rx.rst_fromcontent(...)` / `rst_fromcontent(...)` reading from `binaryFile`-loaded bytes of the source
- Any other GeoBrix function that takes a file path / URI to the source
- Any `dbutils.fs.head` / `binaryFile` / `spark.read.format("binaryFile")` of the source (these read bytes too)

The only operation allowed against the source path before Phase 2a runs is **`dbutils.fs.ls`** itself (it's metadata-only, no byte read) — which is exactly what Stage 1 uses.

#### Phase 2a. Size check (MANDATORY first action)

The size check has **two stages with different jobs**:

- **Stage 1 (universal — every format):** `dbutils.fs.ls` + format detection + total size. It answers a *gate* question — **is GeoBrix even worth it for this source?** — and detects SUBDATASET formats. This is the same metadata-only read the §A/§B gate relies on; nothing here commits you to GeoBrix.
- **Stage 2 (single-grid only):** the byte-size → LARGE vs SMALL/MEDIUM routing that decides *how* to ingest with GeoBrix (standard read vs retile-and-persist). Only runs once you're on the single-grid GeoBrix path.

##### Stage 1 — universal gate: format + size → is GeoBrix worth it?

```python
items = dbutils.fs.ls(source_path)  # single file or directory; metadata only, no byte read

# Detect format FIRST — it decides which gate applies. The byte-size heuristic in Stage 2
# is SINGLE-GRID only (bytes ≈ pixels ≈ processing cost). That assumption is FALSE for
# NetCDF/GRIB/HDF (variables-as-subdatasets, multidimensional: vars × time × level × grid).
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
    # NetCDF/GRIB/HDF — bytes ≠ spatial size, and these are exactly the multi-variable /
    # temporal sources GeoBrix is built for. Skip the 'worth it' gate; go straight to 2d.
    routing = "SUBDATASET"
    print("\n*** ROUTING: SUBDATASET (NetCDF/GRIB/HDF) ***")
    print("Byte-based LARGE/SMALL does NOT apply — bigness is logical (vars × time × level).")
    print("Go to Phase 2d: enumerate subdatasets -> select variable/time -> subset ->")
    print("write GeoTIFF/COG -> re-run THIS size check on that GeoTIFF output.")
else:
    # Single-grid (GeoTIFF/COG, JP2, IMG). Apply the §B scale gate (top of skill) to these
    # numbers before Stage 2: if it's one small scene for a one-off op, GeoBrix may be
    # overkill — surface it and ASK per §B (proceed with GeoBrix, or single-node rasterio?).
    # Otherwise (many scenes / big scene / in-pipeline), continue to Stage 2.
    routing = "SINGLE_GRID_PENDING_GATE"
    print(f"\n*** Single-grid: {n_files} file(s), {total_gb:.2f} GB — apply §B gate before Stage 2 ***")
```

##### Stage 2 — single-grid routing fork (LARGE vs SMALL/MEDIUM)

For single-grid sources committed to GeoBrix (Stage 1 didn't route SUBDATASET, and the §B gate
is satisfied or the user chose GeoBrix), pick the branch by **input** size:

| Condition (input, from Stage 1's numbers) | Routing | Go to |
|---|---|---|
| total > ~5 GB, **or** > 100 files averaging > 500 MB | **LARGE** | Phase 2c — retile-and-persist |
| otherwise | **SMALL/MEDIUM** | Phase 2b — standard read |

🛑 **LARGE → STOP: retile-and-persist is the ONLY approved path.** Do not
`spark.read...load(huge_source)`; do not "optimize" by filtering/clipping/`sizeInMB`-splitting
the source read; do not reason that the *output* will be small (routing is on **input** size).
When unsure between the two branches, choose LARGE. The runnable threshold check, tile-size
tuning, and the full anti-pattern list live in `references/2-ingest/2c-large-raster-retile.md`.

**After Stage 1 runs, look at its output — it drives everything downstream.** Note the detected format(s): if it prints `['unknown']` (e.g. extension-less files), inspect the source manually before assuming the single-grid path — a NetCDF/GRIB file without an extension would otherwise skip the SUBDATASET branch and be byte-routed incorrectly.

#### Hard rules — non-negotiable, no creative interpretation

1. **You must surface both the gate outcome and the routing decision to the human.** After Phase 2a Stage 1 runs, if the single-grid scale gate looks borderline (one small scene), state that and ask whether to proceed with GeoBrix or use a single-node library — don't silently continue. Once routing is settled, state it: "Phase 2a routed this as [LARGE / SMALL-MEDIUM / SUBDATASET]. Proceeding with [retile-and-persist / standard read / NetCDF subset-to-GeoTIFF]." Do not silently transition.

2. **If routing is SUBDATASET (NetCDF/GRIB/HDF), go to Phase 2d.** Do NOT apply the GB thresholds, do NOT spatially retile the raw file, and do NOT read it through Phase 2b/2c as if it were a single grid. The byte-based gate is a GeoTIFF heuristic and is meaningless here — the file's "size" is its logical shape (variables × timesteps × levels), which `dbutils.fs.ls` cannot see.

3. **LARGE-path enforcement (retile-and-persist only, no "smart" source reads, input-not-output size) lives in `references/2-ingest/2c-large-raster-retile.md`** — the teeth above are the summary; 2c has the full anti-pattern list. Read it before writing any LARGE ingest code.

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

If Phase 2a printed `ROUTING: LARGE`, skip Phase 2b and follow the **retile-and-persist pattern** in `references/2-ingest/2c-large-raster-retile.md` — the path for LARGE **single-grid** rasters (GeoTIFF/COG, `.jp2`, `.img`). It handles the LARGE threshold confirmation, read + inspect + auto-sized retile + Delta persist, feeds downstream phases from the persisted table, and carries the **full anti-pattern list** (the shortcuts that still read the giant source — all forbidden). **NetCDF/GRIB are out of scope for that pattern** — they expose variables as subdatasets and their CRS may need checking (sometimes inferred from CF metadata, sometimes absent), so their read + metadata steps differ before any retile.

#### Phase 2d. SUBDATASET routing → NetCDF / GRIB / HDF

If Phase 2a printed `ROUTING: SUBDATASET`, the byte-based size gate does **not** apply and you must **not** feed the raw file into Phase 2b/2c. These formats expose multiple **variables as subdatasets** and are often multidimensional (`time × level × lat × lon`); their CRS may be inferred from CF metadata or absent (check `rst_srid`, don't assume).

> ⚠️ **GeoBrix officially supports GeoTIFF only** (per its readers list — see Resources). NetCDF/GRIB are best-effort via the generic GDAL driver, with no guarantees. The robust path is to reduce them to GeoTIFFs and then use the supported single-grid flow. Because support is best-effort, falling back to a NetCDF-native tool (xarray / rioxarray) for the *extraction* step is more legitimate here than it would be for GeoTIFF — but still surface the choice to the user per the substitution policy.

**Strategy (don't byte-route, don't spatially retile the raw file):**

1. **Enumerate subdatasets / variables** — `rx.rst_subdatasets("tile")` (or `gdalinfo` / xarray) to see the variables and their dimensions. The "size" that matters is the *logical shape* — how many variables × timesteps × levels, at what per-slice grid — not GB on disk.
2. **Select** the variable(s) and time/level slice(s) you actually need; drop the rest. This is usually where the real data-volume reduction happens.
3. **Subset / rechunk** the selected slices. Two viable outputs (the worked example uses the second): convert to GeoTIFF/COG and re-enter the single-grid flow, **or** rechunk to smaller `.nc` files with xarray and read them directly with `driverName="netCDF"`.
4. **Analyse** — check `rx.rst_srid("tile")` first (GDAL often infers the CRS from CF metadata; if it's genuinely `0`, ask the user for the EPSG — don't hardcode). Align to your boundary's CRS with `rx.rst_transform` before clip/H3. Bands map to **timesteps**, not spectral bands.

**Worked example: `references/2-ingest/2d-netcdf-grib.md`** — full two-scenario flow (one big `.nc` → rechunk by a variable → analyse chunked files), with the CRS check/align step, hourly-band explode, clip-to-city, and H3 time-series.

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
# See references/3-process/h3.md for the example types + function list.
```

The snippets above are a quick taste. The worked, format-agnostic analytics live in dedicated docs (they run on the `tile` column from any ingestion route):

- `references/3-process/analytics.md` — per-band stats (incl. multi-temporal), clip to a boundary polygon, zonal stats
- `references/3-process/h3.md` — H3 tessellate / aggregate / time-series, the band-semantics matrix, function list

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
| Lightweight fails on Serverless | Check Serverless env 5+ (Python 3.12); use PEP 508 `%pip` form in `1-install/install-light.md` |
| `pyrx` ImportError after install | Release may lack Lightweight — use Heavyweight on classic x86 or upgrade GeoBrix |
| Heavyweight on Serverless | Impossible — switch to classic x86, or use Lightweight |

## Resources

### References
- `references/1-install/install-light.md` — Lightweight install (Serverless, `%pip [light]`)
- `references/1-install/install-heavy.md` — Install router + Heavyweight cluster setup (JAR, init script, troubleshooting)
- `references/functions.md` — Full RasterX function reference (metadata, transformations, generators, H3 aggregation)
- `references/2-ingest/2c-large-raster-retile.md` — Large **single-grid** raster (LARGE routing) retile-and-persist pattern for GeoTIFF/COG, `.jp2`, `.img`: read → inspect true dimensions → auto-sized retile → Delta persist → H3 aggregation (NetCDF/GRIB out of scope)
- `references/2-ingest/2d-netcdf-grib.md` — NetCDF/GRIB (SUBDATASET routing) two-scenario flow: rechunk one big `.nc` by a variable (xarray), then analyse chunked files with GeoBrix — CRS check/align, hourly-band explode, clip-to-city, H3 time-series
- `references/3-process/analytics.md` — **Phase 3, format-agnostic** analytics on a `tile` column: summary metadata, per-band stats (incl. multi-temporal `band_index → timestamp`), clip to a boundary polygon, zonal stats
- `references/3-process/h3.md` — **Phase 3, format-agnostic** H3 example types on a `tile` column: tessellate, aggregate per cell, multi-temporal time-series, coarse-grid pre-retile, region filtering/rendering; band-semantics matrix + function list

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

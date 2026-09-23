# GeoBrix raster skill — TODO / backlog (internal)

Working notes for what this skill should support next. **Not user-facing** — SKILL.md only
advertises what's been validated here. Two parts: (1) ingestion **format** support, and
(2) processing/ETL **pattern** gaps.

The scale gate (SKILL.md §B) applies throughout: GeoBrix is warranted when work is distributed
— many scenes, one scene too big for a node, or must live in a Spark/UC pipeline. A single
small raster's one-off NDVI/clip is a rasterio-on-one-node job, not GeoBrix.

---

# Part 1 — Ingestion format support

Deciding which raster file formats the skill actively supports for **ingestion**.

## Reader/writer tiers

Two tiers emit the **same `(source, tile)` schema**, so they're drop-in swaps (one-line
`format(...)` change):

- **Lightweight** — `*_gbx` suffix (raster). Pure Python/PySpark, rasterio-backed, JAR-free; Serverless / shared / ARM.
- **Heavyweight** — `gdal` (raster) / `*_ogr` (vector). Native GDAL on the JVM; classic x86 only (JAR + init script).

Full options + examples: geobrix docs → Readers / Writers.

### Raster & tiles — reader/writer names

| Format | Read (light / heavy) | Write (light / heavy) |
|---|---|---|
| Raster (any GDAL driver) | `raster_gbx` / `gdal` | `raster_gbx` / `gdal` |
| GeoTIFF | `gtiff_gbx` / `gtiff_gdal` | `gtiff_gbx` / `gtiff_gdal` |
| PMTiles | — | `pmtiles_gbx` / `pmtiles` |

## Support tiers

Keyed to **actual test coverage in the geobrix repo**, not GDAL's theoretical driver list.
As of review: fixtures exist for GeoTIFF (16), NetCDF (12), GRIB (3+1) only.

### P0 — First-class (done)
| Format | Ext | Read | Write | Notes |
|---|---|---|---|---|
| GeoTIFF | `.tif` `.tiff` | `gtiff_gbx` / `gtiff_gdal` | `gtiff_gbx` / `gtiff_gdal` | Tested & optimized; documented happy path. **Address first.** |
| COG (Cloud-Optimized GeoTIFF) | `.tif` | `gtiff_gbx` / `gtiff_gdal` | via `rst_cog_convert` then write | COG is a GeoTIFF variant; read is the same path. Confirm write/convert story. |

### P1 — Best-effort, tested (subdataset routing)
| Format | Ext | Read | Write | Notes |
|---|---|---|---|---|
| NetCDF | `.nc` | `raster_gbx` / `gdal` (`driver=netCDF`) | `gdal` | SUBDATASET routing (SKILL Phase 2d). CRS often from CF metadata. |
| GRIB / GRIB2 | `.grb` `.grib2` | `gdal` (`driver=GRIB`) | `gdal` | Weather models (HRRR sample data). Heavy tier for driver options. |

### To review — common formats not yet validated
Prioritized by field frequency. Promote to P1 only after: confirm driver present in the
GeoBrix container → add a read + metadata example → verify `tile` schema round-trips.

| Priority | Format | Ext | Why it matters | Status |
|---|---|---|---|---|
| **HIGH** | Zarr | dir | **Very popular** cloud-native array store; growing fast for large EO/climate archives. GDAL has a Zarr driver (≥3.4), so it's a plausible **peer of NetCDF** as a GeoBrix input — NOT categorically out of scope. | Unverified: run `gdal.GetDriverByName("Zarr")` in the container. If present → treat like NetCDF (SUBDATASET routing, array-native caveats). See Part 4 decision gate. |
| **HIGH** | HDF5 / HDF4 | `.h5` `.hdf` | MODIS + many NASA/ESA products; needs SUBDATASET routing. | Not started. |
| MED | JPEG2000 | `.jp2` | Some Sentinel-2 distributions arrive as JP2. | Not started (`JP2OpenJPEG` driver). |
| LOW | VRT (GDAL virtual mosaic) | `.vrt` | Virtual mosaics across many files; useful for large tiled archives. | Not started. |
| LOW | ENVI | `.hdr` + data | Remote-sensing labs; header-based. | Not started. |

---

# Part 2 — Processing / ETL pattern gaps

The E2E arc is **ingest → ETL (chunk/retile, subset) → process → persist → visualize**. Most
stages already exist as example docs; this tracks what's DONE (don't rebuild) vs. what's a GAP.

## Existing patterns — DONE (don't rebuild, link/extend only)

| Stage | Pattern | Where |
|---|---|---|
| ETL: chunk/retile | Retile large raster into smaller tiles → persist to Delta → read back | `2-ingest/2c-large-raster-retile.md` |
| ETL: subset | NetCDF/GRIB subdataset select + reduce to tiles | `2-ingest/2d-netcdf-grib.md` |
| Process | Stats, reproject (`rst_transform`), clip (`rst_clip`), zonal | `3-process/analytics.md` |
| Aggregate | H3 tessellate/aggregate, multi-temporal time-series | `3-process/h3.md` |
| Persist | Write tiles/results to Delta in UC | SKILL.md Phase 4 |

## Gaps to build (priority order)

### 1. Visualization — the missing end of the arc (HIGH)
The arc dead-ends at Delta ("persist"), but the user goal is "…and eventually **visualise**
this." SKILL.md now points at Phase 5 for the final-step viz exception, but there's no worked
`5-visualize/visualize.md` doc yet.

Two candidate paths (not yet investigated — decide later):
- **Repo's own approach** — `geobrix/docs/docs/api/viz.mdx` documents a GeoBrix viz path. Read it and capture the recommendation before choosing.
- **rasterio → numpy → matplotlib/contextily** — the sanctioned final-render exception: read `rst_asformat("GTiff")` bytes → numpy array → render. GeoBrix does all processing; rasterio only bridges bytes→array at the render step.

When writing `5-visualize/visualize.md`, carry over the exception detail that used to live in
SKILL.md's "Where rasterio IS allowed" table:
- Why it's allowed: GeoBrix returns raster tiles; matplotlib needs numpy arrays — rasterio is the standard bytes→array bridge, render step only.
- rasterio is NOT preinstalled on DBR — `%pip install rasterio` + `dbutils.library.restartPython()` (then re-import/re-register GeoBrix), or add to the cluster Libraries tab.
- Surface it: "Using rasterio for the final render only; GeoBrix did the [processing]."

TODO: read `viz.mdx`, decide the path, write `5-visualize/visualize.md` closing the arc.

### 2. Connected E2E walkthrough (MED)
Stages exist as separate docs reached via phase routing; no single runnable narrative —
"TIFF in → retile → NDVI → Delta → map" — top to bottom. TODO: decide where a connected
`end-to-end.md` walkthrough lives (likely `references/` root, since it spans all phases)
and stitch the existing pieces + viz into it.

### 3. Domain patterns → map to existing stages (LOW; write on demand)
Users describe data by mission. Each resolves to an ingest tier + a chain of existing stages,
so most need no new machinery — just a worked example. Only spectral indices need new
functions in `functions.md`.

| Domain pattern | Ingest tier | Stages it uses | New work needed? |
|---|---|---|---|
| **Earth observation imagery** (Sentinel-2, Landsat, MODIS, VIIRS, Planet) | GeoTIFF P0 (MODIS = HDF, see Part 1) | ingest → retile → band stacking → indices | Band-stacking example |
| **Vegetation / water / fire indices** (NDVI, NDWI, EVI, NBR — `(band−band)/(band+band)`) | P0 | ingest → index function | Add EVI/NDWI/NBR/SAVI to `functions.md` (only `rst_ndvi` there today) |
| **Elevation models** (DEM, DTM, DSM) | GeoTIFF P0 | ingest → terrain (slope/aspect/hillshade) | Add terrain functions to `functions.md` first |
| **Land cover / land use** (categorical pixels) | P0 | ingest → zonal/majority → `rst_polygonize` | Categorical-aggregation example |
| **Weather & climate grids** (temp, precip, wind over time) | GRIB/NetCDF P1 | subset → temporal bands → aggregate | Partial (`2-ingest/2d-netcdf-grib.md`) |
| **Nighttime lights / urbanization** (VIIRS DNB, DMSP-OLS) | P0 | ingest → zonal / H3 aggregation | Example only |

---

# Part 3 — Tier consistency (example docs hardcode Heavy)

Moved from SKILL.md's former "Outstanding (deferred)" note. The example docs and some SKILL
snippets predate the Lightweight-default policy and still hardcode the Heavy tier.

### 1. Light tier in example docs (MED — partially done)
Phase 3 `rst_*` analytics are tier-agnostic, but example docs and the Phase 2b/3 snippets
still hardcode Heavy (`rasterx`, `gtiff_gdal`/`gdal`). Status:
- ✅ **`2-ingest/2c-large-raster-retile.md` DONE** — now fully tier-aware: reuses Phase 1 `rx`/`GEOTIFF_READER`/`GENERIC_READER` (no Heavy re-import), tier-gated reader, and the retile step branches Heavy (column+`explode`) vs Light (`LATERAL gbx_rst_retile`). Also: the generator Light-vs-Heavy invocation rule now lives once in `functions.md` (generators are `LATERAL` UDTFs on Light, column+`explode` on Heavy — raises `NotImplementedError` if called column-style on Light).
- ✅ **`3-process/analytics.md`, `3-process/h3.md` DONE** — Phase 1 Lightweight (`pyrx`) bootstrap as default; Heavy noted as equivalent only when that tier is installed.
- ✅ **`SKILL.md` Phase 2b + Phase 3 DONE** — Phase 2b uses `GEOTIFF_READER` from Phase 1; Phase 3 examples default to `pyrx` (Lightweight), with Heavy as a comment-only equivalent.
- Leave `2-ingest/2d-netcdf-grib.md` heavy-only until the NetCDF light path is defined (item 2).

### 2. NetCDF/GRIB tier routing + a "NetCDF on Light" gate (MED)
Define upfront tier routing for NetCDF/GRIB and the xarray→GeoTIFF→light path. Until then,
`2-ingest/2d-netcdf-grib.md` stays heavy-only.

**Empirical findings from the geobrix source (0.4.0) — what's actually true about NetCDF per tier:**
- **Heavy (`gdal` reader):** full, documented, tested NetCDF path — `driverName="netCDF"`, `readSubdatasets`. This is what the Rabobank notebook uses, and it's the aligned/recommended path.
- **Light (`raster_gbx` reader):** the reader is rasterio-backed with **no extension filter** — so it *can* physically `rasterio.open()` a `.nc`. BUT: (a) it has a **GTiff-only fast path** (`if whole and driver == "GTiff"`); NetCDF falls through to a best-effort decode/re-encode; (b) the reader has **no explicit subdataset handling** — it reads whatever rasterio exposes as the *default* dataset, so multi-variable `.nc` may silently surface only one variable; (c) **no integration test** was found for `spark.read.format("raster_gbx").load("*.nc")` — the `rst_subdatasets`/`rst_getsubdataset` funcs are tested on Light, but via a `tile_from_path` test helper, not the reader.
- Net: NetCDF-on-Light is **plausible but unverified and un-subdataset-aware** — matches the repo's "GeoTIFF-first, others best-effort" stance, visible directly in the reader code.

**TODO — write a "NetCDF on Light: use vs. don't" gate** (to sit in 2d or the Part 4 gate):
| Use NetCDF on **Light** (`raster_gbx`) IF… | Do NOT — use **Heavy** (`gdal`) instead IF… |
|---|---|
| Single-variable `.nc`, default dataset is the one you want | Multi-variable / multi-subdataset file (Light reader may grab only the default var) |
| You've **verified** the read: `rst_numbands` + `rst_subdatasets` match the xarray inspection | You need `readSubdatasets` / explicit subdataset selection (Heavy-only option) |
| Serverless-only constraint forces Light, AND the above holds | You want the tested/documented path (Heavy is it for NetCDF) |
| Exploratory / one-off | Production or anything relied upon |

Preconditions before this gate can be documented as real: (1) run `spark.read.format("raster_gbx").load(<.nc>)` on a cluster and confirm band/variable behavior vs. xarray; (2) confirm whether `.option("driverName","netCDF")` is honored by `raster_gbx` (the Rabobank notebook used it on **Heavy** `gdal`, not Light — do NOT assume it carries over). Until (1)+(2) pass, 2d stays Heavy-only and the gate says "NetCDF → prefer Heavy; Light is verify-first best-effort."

---

# Part 4 — NetCDF / Zarr decision gate (DRAFT — not yet in SKILL.md)

Ready-to-insert gate for when a user points at NetCDF (or Zarr). Two sequential decisions:
engine first, then reader. Decided placement/wording TBD — candidates: a subsection under
"When to Use" after §B, or the top of Phase 2d. Array→xarray outcome should **decline + point
to xarray** (out of scope), per the §B honesty gate.

**Key framings established (don't lose these):**
- **Array vs raster is a *lens on the same file*, not a property of the file.** NetCDF/Zarr are array-native (`var × time × level × lat × lon`); GDAL/GeoBrix coerce them into a raster (flattens labeled dims → anonymous bands, which is why `timestampadd(HOUR, band_index, ...)` reconstruction is needed). GeoTIFF is raster-native.
- **The intuitive trigger = "overlay":** does the operation combine the data with *another spatial thing* (polygon/other raster/CRS/points)? Yes → raster → GeoBrix. Stays inside its own grid (time-series, ensemble stats, ML features) → array → xarray.
- **Zarr is NOT xarray-only.** GDAL has a Zarr driver, so Zarr can feed GeoBrix too (unverified in container). The `.nc`-vs-Zarr choice is **amortization**, not capability.
- **COG is the raster world's cloud-optimized format; Zarr is the array world's.** Don't convert `.nc → Zarr` to feed GeoBrix as a "speed" move — if GeoBrix reads slow, reduce to COG. (COG is irrelevant to the `.nc`-vs-Zarr-as-GeoBrix-input question — don't muddy it in.)

### Decision 1 — is this a GeoBrix (raster) workload at all?
| Operation | Lens | Engine |
|---|---|---|
| Clip to polygon; reproject CRS; raster→H3; overlay/join another raster; sample at points | raster / overlay | ✅ GeoBrix → Decision 2 |
| Select by coord/time; reduce over own axes (mean/max over lat-lon or time); ensemble math; ML features | array | ❌ Not GeoBrix. State plainly: *"This is array/time-series work — xarray (+ Zarr/Kerchunk for cloud-read speed) is the right tool, not GeoBrix."* Then stop; xarray is out of scope. |

### Decision 2 — (GeoBrix only) read `.nc` directly, or convert to Zarr first?
Both GDAL-readable into the `tile` column. Choice = amortization, not capability.
First verify the driver: `assert gdal.GetDriverByName("Zarr") is not None` (else read `.nc` directly).

| Read `.nc` directly | Convert to Zarr first |
|---|---|
| One-off / exploratory / small | Data reused repeatedly by many jobs/users |
| Few files, latency not a problem | Many files AND cloud-read latency is a *measured* bottleneck |
| Zarr driver unverified | Zarr driver confirmed present |
| NetCDF is the tested GeoBrix path (12 fixtures) | You also want xarray access to the same data |

**Rule:** default to `.nc` direct. Convert to Zarr only when reads are frequent AND latency is proven AND the driver check passes. A single benchmark run does NOT justify conversion.

**Band-semantics verify (both readers):** GDAL discards labeled axes, so "band N = hour N" is an assumption. Compare `rst_numbands("tile")` against the source's `time`/`level` dims (xarray inspection) before mapping `band_index` to time. Also set `.option("readSubdatasets","true")` explicitly for multi-variable files — omitting it can silently read only one variable.

---

# Part 9 — retile read: set `sizeInMB="512"` (the tested value)

**Tested fact (from the working London notebook):** read
`spark.read.format("gtiff_gdal").option("sizeInMB","512").load(...)` → retile (`withColumn`) +
`saveAsTable` succeeded on the 10.8 GB VIIRS file. So the skill's 2c read sets `sizeInMB="512"`.

**What is NOT established (don't re-add):**
- I earlier added a `getNumPartitions()` / `assert num_parts > 1` "OOM guard" — **remove/never add it.** It was inference, and it *blocked* a read the working notebook ran fine: the notebook reads this file at a low partition count with `sizeInMB=512` and proceeds without issue. A low partition count is **not** itself a failure.
- Other `sizeInMB` values (e.g. 128) may work but are **unverified** — only 512 is tested. Smaller values *plausibly* help OOM (more/smaller chunks) but that's a hypothesis, not tested.

This is the `withColumn` Heavy retile (tested path); Light retile remains unverified (Part 8).

---

# Part 10 — Zonal-stats pruning guidance (⚠️ ADVISORY in analytics.md — verify at scale)

Rewrote `3-process/analytics.md` § "Zonal statistics" (was a 5-line hand-wave) into a real
pruning-strategy section, driven by the VIIRS multi-city (`gold_cities`) workflow. **All of it
is advisory / not-yet-verified-at-scale** — nothing here has run at the full 119K `gold_cities`.

**What was corrected (don't regress):**
- The earlier claim "a bbox match on plain DOUBLE columns does NOT prune" was **WRONG** and is
  removed. Databricks' **range-join optimization (`RangeJoin`) is auto-enabled in Databricks SQL**
  — `crossJoin(...).filter(inequalities)` CAN prune, binning a numeric axis then re-checking. The
  honest caveat is that range-join is **1D** (bins one axis; a 2D bbox overlap isn't a true 2D
  index), tunable via the `RANGE_JOIN(alias, binSize)` hint.
- Also: `BroadcastNestedLoopJoin` **evaluates** every pair (O(N×M) comparisons) but **streams**
  output — it does NOT materialize the full cross-product as a table. (My "materializes all pairs"
  wording was wrong.)

**Three options documented, in reach-for order:**
1. **Doubles + inequalities** → relies on `RangeJoin` (1D). Try first; check `.explain()`.
2. **Option A — `ST_Intersects` on `GEOMETRY`** → Photon's 2D Spatial Join operator (bbox
   prefilter + exact recheck). Requires Photon + native `GEOMETRY`/`ST_` support (DBR 17.1+).
3. **Option B — H3 equi-join** → tessellate both sides, hash-join on cellID, refine. For extreme
   scale / when neither prunes. Points to `h3.md` primitives (`rst_h3_tessellate`,
   `h3_polyfillash3`) — the *recipe* (tessellate→equi-join→refine) is NOT written into h3.md
   because it's unverified; kept advisory in analytics.md only.

**VERIFIED on the demo warehouse (DBSQL, `862f1d757f0424f7`) — safe to state:**
- `ST_Intersects`, `ST_GeomFromText`, `ST_Point(lon,lat)`, `ST_MakeEnvelope(xmin,ymin,xmax,ymax)`
  all resolve and run. `ST_MakeEnvelope` is the clean bbox-doubles→polygon path (hand-building a
  WKT string via `format_string` FAILED with a Decimal type error — don't do that).
- `gold_cities` schema: `country, city_name, center_lon, center_lat, has_polygon, geom_wkt,
  bbox_xmin/xmax/ymin/ymax` — so all three geometry-construction forms (WKT / envelope / point)
  map to real columns.

**Open questions this must answer before ANY of it becomes "tested" (do NOT promote until then):**
- **`rst_boundingbox(tile)` output type** — the draft uses `ST_GeomFromWKB(rst_boundingbox(tile))`
  following the VIIRS notebook's `st_geomfromwkb(...)` treatment (implies WKB out). functions.md
  line 29 says it returns "a geometry." Confirm on the cluster whether it's raw `GEOMETRY` (no
  wrapper) or WKB (needs `ST_GeomFromWKB`) before committing the exact call.
- **Does Photon's Spatial Join actually engage** on GeoBrix-derived tile geometries at 119K scale,
  or fall back to `BroadcastNestedLoopJoin`? Only `.explain()` on a real run answers this. The
  whole "Option A pays off" claim is gated on this.
- **Which option actually wins** at 119K `gold_cities` — doubles+RangeJoin vs Option A vs H3.
  Whichever runs green + prunes (confirmed by `.explain()`) gets upgraded from "recommended
  pattern" to "tested"; the others stay advisory.

**Note:** the `.explain()`-confirmation instruction and the "don't cargo-cult the geometry cast"
warning (geometry is MORE expensive per-row than doubles if the operator doesn't engage) are baked
into the analytics.md section — keep them; they're the honest guardrails.

---

# Part 8 — ⬜ UNVERIFIED: Light-tier ingestion + retile on a non-striped raster (TO TEST)

**Registration finding (from repo source + runs):** `gbx_rst_*` **SQL** names only exist after
`register(spark)` runs `spark.udtf.register(...)` — the Python column API (`rx.rst_*`) does NOT
need that. On our cluster the SQL name `gbx_rst_retile` did NOT resolve
(`UNRESOLVABLE_TABLE_VALUED_FUNCTION`) → SQL registration didn't take / wasn't run for the tier in
use. **Decision (user): pick ONE surface and don't swap — default Python column API.** Skill now
tells the agent: Heavy → `rx.rst_retile` `withColumn` (tested); never route Heavy through SQL.
Light generators raise `NotImplementedError` on the Python column call, and their SQL `LATERAL`
path depends on a successful `register(spark)` — so Light retile stays UNVERIFIED until the test
below confirms whether `register(spark)` makes `gbx_rst_retile` resolvable on Light.


**Nothing in the Light ingestion/retile path has been run green this session.** The only tested
GeoBrix run was **Heavy** (VIIRS notebook: `withColumn(rx.rst_retile(...))` + `saveAsTable`).
The Light path was written from inference/the library docstring and has failed twice on real
runs — so per the tested-only rule it must be treated as unverified until a clean run.

**To test — run the base pattern on a Light-friendly source** (Serverless + Lightweight, a
**non-striped / tiled GeoTIFF or COG**, so the striped-OOM confound is removed):
1. Phase 1 Light bootstrap (`pyrx`, `gtiff_gbx`/`raster_gbx`).
2. Read a tiled/COG GeoTIFF via `gtiff_gbx`.
3. Retile + persist to Delta on Light.

**Open questions this run must answer (do NOT write answers into the skill until confirmed):**
- **Light retile invocation form.** The docstring says `SELECT t.* FROM <df>, LATERAL gbx_rst_retile(tile, w, h) t`, and the skill uses that — but a real run threw `[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve rst_retile ...` (note: error names bare `rst_retile`, not `gbx_rst_retile`). Two unresolved possibilities:
  - (a) the call needs the `gbx_` prefix (`gbx_rst_retile`) — a name issue, easily fixed; or
  - (b) the Light Python UDTF is **not registered as a SQL TVF** on Serverless / Spark Connect at all — a real limitation, in which case Light retile via LATERAL doesn't work on Serverless and the honest guidance is "use Heavy for retile."
  Earlier the column-style call raised `NotImplementedError`, so both column-style AND the LATERAL form have failed so far. **Determine which of (a)/(b) is true on a cluster.**
- Whether `gtiff_gbx` reads the tiled source cleanly on Light (no OOM, since non-striped).
- Confirm the persisted Delta table round-trips (`spark.read.table` → usable `tile` column).

**Until verified:** the skill's Light retile snippet (`LATERAL gbx_rst_retile`) is **suspect** —
it may be wrong. Heavy retile is the only tested path. Consider marking the Light retile block in
`2c` as "unverified — see todo Part 8" so it isn't trusted as-is. (Beatrice will run this later.)

---

# Part 6 — Small-AOI escape at §B (✅ APPLIED to SKILL.md)

Found via the VIIRS/London run: the agent's goal was "visualize London (one small AOI) from a
10.8 GB global scene, once." Phase 2a routed LARGE on **input** size and its "don't reason about
output size" rule **overrode the §B single-node gate**, forcing the agent toward retile/COG
(15–30 min) — when a `rasterio` windowed read of London's bbox pulls only the overlapping strips
(~hundreds of MB) on a single node. The agent correctly reasoned this out but felt trapped by the
skill's anti-pattern rule.

**Fix applied:** added **§B1 "Small-AOI escape"** — a large *source* does not force GeoBrix; if
the *entire deliverable* is one small AOI produced once (no whole-raster processing), route to a
single-node **windowed read** (it's the §B single-node case AND the allowed viz/metadata
exception). Guardrail: the escape applies ONLY when the small AOI is the whole job — a
distributed job that merely *ends* with a clip still retiles (input-size rule intact). Added a
cross-reference note in Phase 2a's 🛑 LARGE block so the two rules don't contradict at the point
of use. This closes the loophole-vs-trap tension: §B1 decides *before* Phase 2a; if you reach the
LARGE branch, the escape didn't apply and retile is mandatory.

**Reference:** VIIRS/London run (source `VNL_npp_2024...median_masked.dat.tif`, London bbox viz).

---

# Part 7 — Large-striped-read guidance leans HEAVY (✅ APPLIED to SKILL.md + 2c)

Follows the soft-advisory approach (Part 5 hard gating stays parked), but makes the advice
**opinionated toward Heavy** based on empirical experience: **COG conversion often takes longer
than the entire Heavy read.** Rationale = pass-count — Heavy is one full pass (read+retile+persist
→ Delta); COG-on-Light is two (full rewrite to COG, *then* read+retile+persist → same Delta). COG
does strictly more work for the identical result, and in the retile flow the reusable artifact is
the Delta table, not the COG, so the "reuse amortizes the rewrite" argument rarely applies.

Applied: `2c-large-raster-retile.md` § "If a large whole-raster read struggles on Light" now
**recommends Heavy**, demotes COG to "Serverless-locked / COG-reused fallback, usually slower for
one-off," with a user-facing script that leans Heavy. SKILL.md gate heads-up updated to match
("prefer switching to Heavy … COG-convert only when Heavy unavailable"). Still user-decides — not
a hard gate.

---

# Part 5 — Large striped GeoTIFF OOMs on LIGHT but not HEAVY (⏸️ BACKED OUT of SKILL.md — parked here)

> **Status: PARKED (2026-07-28).** The striped gating was removed from SKILL.md's decision path
> at user's request — for now the skill uses the plain default (try Lightweight, escalate to
> Heavy for documented heavy-only surfaces), so the user can observe how that base pattern
> behaves without the striped detour. **Nothing is lost** — the exact removed SKILL.md text is
> parked verbatim below (§ "Parked verbatim") for a clean paste-back. The 2c COG-convert
> section was LEFT in place (valid reference technique; just no longer gated to from Stage 3).
> Findings/rationale retained below.

### Parked verbatim — removed SKILL.md blocks (paste back to restore)

**(A) Heavy-routing list clause** — in "Route to Heavyweight when ANY apply", the trailing item was:
> `, or a **LARGE internally-striped single-grid raster** (see the mandatory pre-install layout probe below)`
(Removed; the list now ends at "`pyrx` unavailable in the installed release.")

**(B) Pre-install layout probe block** — sat between the "Route to Lightweight" line and "Compute routing:":
```markdown
**🛑 Mandatory pre-install layout probe (LARGE single-grid GeoTIFF).** If the source is a large
single-grid GeoTIFF, check its internal layout **HERE, before choosing/installing a tier** — the
check is a header-only `rasterio` read (metadata, no GeoBrix, no byte read), so it costs nothing
and it decides the tier. Do NOT defer this to Phase 2a and discover it after installing Light.

```python
import rasterio   # pip install rasterio if not present; header read only
with rasterio.open(source_path) as src:
    is_tiled = bool(src.profile.get("tiled", False))
    block = src.block_shapes[0]        # (1, width) == STRIPED; (256,256)/(512,512) == tiled
print(f"tiled={is_tiled}, block_shape={block}")
```

- **Large + striped → prefer Heavyweight.** This is the recommended, straightforward path:
  native GDAL (`gtiff_gdal`) streams the striped read; the Light `gtiff_gbx`/rasterio reader
  OOMs on it at any `sizeInMB`. Default here unless Light is required for another reason
  (Serverless-only, an already-Light cluster, other Light-only work in the same job).
- **Large + striped, but Light is required anyway →** stay Light and **COG-convert the source
  first** (streaming, bounded memory), then retile the COG — see Phase 2a Stage 3 and
  `references/2-ingest/2c-large-raster-retile.md`. This is the fallback, not the default.
- **Tiled / COG (either tier) →** no constraint from layout; follow the normal Light-default.
```

**(C) Phase 2a Stage 3** — the entire "##### Stage 3 — LARGE single-grid on Light: striped → COG-convert first" subsection (the intro paragraph, the `rasterio` layout check, and the tiled/striped+Light/striped+Heavy bullets). Removed in full.

**To restore:** paste (A) back onto the Heavy-routing list, (B) back between "Route to Lightweight" and "Compute routing:", and (C) back as a Stage 3 after the Stage 2 fork in Phase 2a. All three were mutually consistent as of this parking.

---

### Rationale (retained)


**Root cause CORRECTED after comparing two real runs on the SAME file** (VIIRS nighttime
lights, `VNL_npp_2024...median_masked.dat.tif`, 10.8 GB, striped: `is_tiled=False`, block
`(1, 86401)`). The earlier "striped → always OOM" framing was WRONG — the file is striped in
**both** runs, and Heavy handled it fine. The real discriminator is the **tier/reader**:

| | Worked | Failed |
|---|---|---|
| Notebook | "VIIRS Nighttime Lights London GeoBrix" | the pasted Light retile |
| Tier / reader | **Heavy** — `rasterx`, `gtiff_gdal` (native GDAL on JVM) | **Light** — `pyrx`, `gtiff_gbx` (rasterio/Python) |
| File | same striped 10.8 GB VIIRS | same file |
| sizeInMB | 512 | 16, then 1621 (both OOM) |

**Corrected root cause:** on a large **striped** GeoTIFF, a windowed read must pull full-width
strips (`(1, 86401)` blocks), so each read materializes far more than the tile window implies.
**Native GDAL (Heavy `gtiff_gdal`) streams this within memory; the Python/rasterio Light reader
(`gtiff_gbx`) does not — it OOMs**, at any `sizeInMB`. So it's a **tier × striped interaction**,
NOT "striped always fails." Striping is why the read is heavy; the **reader engine** decides
whether that's survivable.

**Two valid fixes (different from what Part 5 originally claimed):**
1. **Use Heavy** for large striped single-grid rasters — native GDAL handles the striped read. Simplest when a classic x86 cluster is available.
2. **Light workaround:** convert striped → **tiled COG** first (`rio-cogeo`, one streaming pass, bounded memory — ships as a GeoBrix `[light]` dep), then retile the COG. COG's internal blocks make windowed reads cheap enough for the Python reader. This is a **Light-tier workaround**, NOT a universal "striped needs COG" rule — on Heavy it's unnecessary.

**Why this is a skill gap:** Phase 2a routes LARGE on **bytes + format only** — it never inspects
internal layout OR factors in the tier. So a large striped GeoTIFF on **Light** goes down the
retile path and OOMs at the read, with no guard. The check is cheap, GeoBrix-free:
```python
import rasterio
with rasterio.open(source_path) as src:
    is_tiled = src.profile.get("tiled", False)
    block_shapes = src.block_shapes        # [(1, width)] == striped
```

**TODO — add a striped × tier guard to SKILL.md** (decision layer, before the retile commits):
- In **Phase 2a Stage 2 / LARGE branch**: after routing LARGE, do the `is_tiled` header check.
  Decision depends on **TIER**:
  - **Heavy + striped** → proceed straight to retile (native GDAL handles it; no COG needed).
  - **Light + large + striped** → either **(a) switch to Heavy**, or **(b) COG-convert first, then retile**. A direct `sizeInMB` read WILL OOM — don't.
  - **tiled/COG (either tier)** → retile directly.
- Add the **COG-convert branch** to `2-ingest/2c-large-raster-retile.md` as the *Light + striped* path (rio-cogeo, streaming) → tiled `.tif` → resume retile. Frame as **allowed preprocessing** (like the metadata header read), not a rasterio substitution — GeoBrix still does all raster work.
- Note in the tier-selection section: **large striped single-grid rasters are a reason to prefer Heavy** (add to the "Route to Heavyweight when ANY apply" list). Many published GeoTIFFs (VIIRS/DMSP nighttime lights) ship striped.

**References:** worked (Heavy) = "Geospatial/Site selection/VIIRS Nighttime Lights London GeoBrix"; failed (Light) = notebook 2118626247380382. Both read the same striped VIIRS file.

---

## Rules when building any of the above
- **GeoTIFF/COG first**: confirm write + `rst_cog_convert` path end-to-end; document read/write reader names in `functions.md`.
- Verify every `rst_*` name against the Scala source before documenting. Don't reference functions not yet in `functions.md` (terrain, EVI/NDWI/NBR/SAVI, quadbin, COG convert exist in the repo but aren't in the reference doc yet — add them as part of writing the pattern).
- Keep the scale gate visible in each pattern.
- When a format is promoted to P1: add it to the SKILL.md "When to Use" ingestion table AND Phase 2a extension detection.
- When a pattern gap is closed: link the new doc here and, for viz, replace the "deferred" note in SKILL.md §B.

## Open questions
- [ ] Zarr: does the container's GDAL build include the Zarr driver? Light (rasterio) vs heavy (gdal) — which reads it?
- [ ] HDF5: SUBDATASET routing already exists (Phase 2d) — validate with a real MODIS `.hdf`, add an example.
- [ ] Decide whether PMTiles (write-only) deserves its own output pattern (ties into visualization).

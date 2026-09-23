# Raster analytics (Phase 3, format-agnostic)

Runs on a `tile` column from **any** ingestion route — Phase 2b (standard read), 2c (retile-and-persist), or 2d (NetCDF/GRIB reduced to tiles). Nothing here is format-specific.

Each section explains a function: **what it does, when to use it, and which parameters are yours to choose.** Pick only what the workflow needs (none are required), and treat every literal value (CRS code, region, band layout) as an example. For H3, see `h3.md`.

The `rst_*` functions are **tier-agnostic** — identical API on Light and Heavy. Reuse the `rx`
already bootstrapped in Phase 1 (Lightweight `pyrx` by default); don't re-import a specific tier here.

```python
# Lightweight (default) — already set up in Phase 1:
from databricks.labs.gbx.pyrx import functions as rx
import pyspark.sql.functions as F
rx.register(spark)
# Heavyweight equivalent (only if that's your installed tier): from databricks.labs.gbx.rasterx import functions as rx
```

## Summary metadata (sanity check)

**What:** per-tile shape and CRS. **When:** almost always first — to confirm dimensions, band count, and CRS before deciding what else to do.

```python
meta = tiles.select(
    "source",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("bands"),
    rx.rst_srid("tile").alias("srid"),       # 0 means no CRS recognized — see rst_transform below
    rx.rst_summary("tile").alias("stats"),
)
```

## Per-band statistics — `rst_avg` / `rst_min` / `rst_max` / `rst_median` / `rst_pixelcount`

**What:** per-band reduction of each tile. **When:** quick value ranges, QA, or building a stats table.

> ⚠️ **Be aware: these return `ARRAY<DOUBLE>` (one value per band), not a scalar.** So aliasing
> `rx.rst_avg("tile")` as `avg` and then doing `F.avg("avg")`/`sum`/`max` across rows throws
> `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE ... ARRAY<DOUBLE>`. **Extract the band(s) you want
> before aggregating** — how depends on your raster (below), don't blanket-assume one form.

- **Single-band** (VIIRS, DEM, …): index the one band, e.g. `rx.rst_avg("tile")[0]` → a `DOUBLE`
  you can then `F.avg`/`F.sum`/`F.max` across tiles.
- **Multi-band**: keep the array and explode per band (next section), or index the specific band
  you mean (`[i]`). Which band is meaningful is a property of *your* source — don't default to `[0]`.

**Multi-band / temporal stacks** — one row per band via `arrays_zip` + `posexplode`. `band_index` is the band ordinal; if your bands are timesteps, map it to time using **your** data's encoding (the formula below is one example, not a default):

```python
(tiles
  .select("source",
      F.posexplode(
          F.arrays_zip(
              rx.rst_avg("tile").alias("avg"),
              rx.rst_max("tile").alias("max"),
              rx.rst_min("tile").alias("min"),
              rx.rst_pixelcount("tile").alias("pixelcount"))
      ).alias("band_index", "stats"))
  # If (and only if) bands are temporal, turn band_index into a timestamp per your data, e.g.:
  # .withColumn("timestamp", F.expr("timestampadd(HOUR, band_index, <step_zero_timestamp>)"))
).display()
```

> Whether a band is a spectral channel or a timestep is a property of the **source**, fixed at ingestion. The explode mechanics are identical; only the meaning of `band_index` differs.

## `rst_transform` — reproject to a target CRS

**What:** reprojects tile pixels to a target EPSG code. **When:** you need the raster in a specific CRS — to match a boundary or table you'll clip/join against, or a map projection (e.g. `3857` for web tiles).

**Check the source CRS first — don't hardcode a target blindly:**

```python
tiles.select(rx.rst_srid("tile").alias("srid")).distinct().show()
```

- **Valid SRID** → reproject only if you actually need a *different* CRS: `rx.rst_transform("tile", F.lit(<target_epsg>))`.
- **`srid` is `0` (missing)** → the source CRS is unknown. Don't assume one. **Confirm the actual CRS with the data owner** (CF lat/lon NetCDF is usually 4326, but projected/rotated grids are not), then align to the confirmed code.

```python
reprojected = tiles.withColumn("tile", rx.rst_transform("tile", F.lit(target_epsg)))   # target_epsg is YOUR choice
```

## `rst_clip` — crop to a boundary polygon

**What:** crops each tile to a polygon. **When:** you only need a spatial subset (an area of interest) — any polygon, from any source.

**Parameters:** the clip geometry must be a **WKT string or WKB** — *not* a UC `GEOMETRY` or `ST_GeomFromText(...)` output. `cutlineAllTouched` (bool): `True` keeps any pixel the boundary touches, `False` only fully-inside pixels.

```python
# boundary_wkt: any polygon WKT — a literal, or pulled from a table. Not tied to any specific region.
boundary_wkt = (spark.read.table("<catalog>.<schema>.<boundaries>")
    .filter(<your row predicate>)
    .select("<wkt_column>").collect()[0][0])

clipped = (tiles
    # tile and boundary MUST share a CRS — align first if needed (see rst_transform)
    .withColumn("clipped", rx.rst_clip("tile", F.lit(boundary_wkt), F.lit(True)))
    .withColumn("avg",     rx.rst_avg("clipped"))   # ARRAY<DOUBLE> per band — index the band you need before aggregating
    .withColumn("bbox_wkb", rx.rst_boundingbox(F.col("clipped")))
    .select("source", F.expr("st_geomfromwkb(bbox_wkb)").alias("bbox"), "avg"))
```

## Zonal statistics (per region)

**What:** a statistic per polygon, across many regions (mean/sum/max radiance per city, county, watershed, zone). **When:** one raster (or a retiled raster table) + a table of many boundaries → one stats row per boundary.

Shape: **prune** (which tiles touch which regions) → **clip** (`rst_clip`) → **reduce** (`rst_avg`/`rst_summary`) → **aggregate** per region key → persist (**Phase 4**).

### rst_* usage in this flow — the three that bite

- **`rst_clip` takes WKT/WKB, not `GEOMETRY`.** Pass the region's WKT string (or WKB), *not* a UC `GEOMETRY` or `ST_GeomFromText(...)` output. Tile and clip geometry **must share a CRS** — align with `rst_transform` first (check `rst_srid("tile")`).
- **Stats return `ARRAY<DOUBLE>` (per band), not scalar.** Index the band before aggregating across rows: single-band `rx.rst_avg("clipped")[0]`; multi-band explode/index. Don't blanket-`[0]`.
- **A region can span multiple tiles** → reduce per-tile, then aggregate per region key. Plain `avg` of pre-averaged tiles is only exact with equal pixel counts; else weight by `rst_pixelcount`, or combine aligned tiles first with the DataFrame aggregates (`rst_combineavg_agg` / `rst_merge_agg`) then reduce once.

### The pruning step — don't assume it brute-forces, and don't assume geometry is free

One / a handful of regions: the tile×region cross is fine, don't over-engineer. **At many regions (10³–10⁵) the pruning step dominates** — but the naive-looking form is not automatically doomed:

> **An inequality bbox match on plain `DOUBLE` columns CAN prune — via range-join optimization.**
> `crossJoin(...).filter(tile_xmax > bbox_xmin AND tile_xmin < bbox_xmax AND ...)` is a non-equi
> join, but Databricks' **range-join optimization (`RangeJoin`) is auto-enabled in Databricks SQL**:
> it bins a numeric axis to prune candidates, then re-checks the predicate. **Caveat: it's 1D** — a
> 2D bbox overlap is pruned on effectively one axis, not a true 2D index. Tune with the
> `RANGE_JOIN(alias, binSize)` hint if auto bin-sizing underperforms.

**Confirm what actually happened — always `.explain()`:** look for `RangeJoin` (doubles path) or a spatial-join operator (geometry path). If you see `BroadcastNestedLoopJoin`, nothing pruned — fix it.

**Option A — native spatial join (Photon), 2D-aware.** Convert both sides to `GEOMETRY`; `ST_Intersects` lets Photon run its Spatial Join operator (bbox prefilter + exact recheck). Requires Photon + native `GEOMETRY`/`ST_` support (DBR 17.1+). Build geometry from **whatever the region table actually has** — all three verified on DBSQL:

```python
# 1. WKT polygon column (when present):     ST_GeomFromText(geom_wkt)
# 2. bbox as 4 doubles → rectangle polygon: ST_MakeEnvelope(bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax)
# 3. lon/lat point (centroid-only regions): ST_Point(center_lon, center_lat)
regions = spark.table("<catalog>.<schema>.<boundaries>").withColumn(
    "geom", F.expr("ST_MakeEnvelope(bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax)"))   # pick the form your data has

# tile side: rst_boundingbox → geometry (returns geometry/WKB — the VIIRS notebook wraps it as WKB)
tiles_g = tiles.withColumn("tile_geom", F.expr("ST_GeomFromWKB(rst_boundingbox(tile))"))

candidates = tiles_g.join(regions, F.expr("ST_Intersects(tile_geom, geom)"))
candidates.explain()   # confirm a spatial join, NOT BroadcastNestedLoopJoin
```

> ⚠️ **Don't cargo-cult the geometry cast.** If `.explain()` still shows `BroadcastNestedLoopJoin`
> (Photon off, unsupported runtime, or the planner couldn't convert the predicate), the cast bought
> readability but **not** pruning — and per-row it's *more* expensive than the doubles comparison,
> which at least gets `RangeJoin`. Then prefer the doubles path or Option B.
>
> *Status: recommended pattern; the `ST_Intersects`→SpatialJoin path on GeoBrix-derived tile
> geometries is **not yet verified at scale** (e.g. 119K regions). Confirm via `.explain()` and a
> real run before treating as tested.*

**Option B — H3 (or coarse-grid) equi-join, for extreme scale or when neither prunes.** Tessellate both sides to an H3 cell id, **equi-join on the cell** (a hash join — genuinely selective), then refine with the exact predicate to drop false positives. The cell primitives live in `h3.md`: `rst_h3_tessellate` (tile → cells) and `h3_polyfillash3(boundary_wkt, res)` (region → covering cells); resolution choice is discussed there too. Coarser res → less duplication but a bigger candidate set; finer → more selective but more rows. Sample cells-per-region to pick.

> *Status: recommended pattern, not yet verified at scale here.*

**Which to reach for:** few regions → don't over-engineer. Many regions → try the doubles form first and check `.explain()` for `RangeJoin`; if its (1D) selectivity is weak and Photon is available → Option A, confirmed by `.explain()`; extreme scale or nothing prunes → Option B (H3). `F.broadcast(small_side)` is a minor extra lever when one side is genuinely tiny (~hundreds of rows). Output is keyed by region → persist per **Phase 4**.

---

See `references/functions.md` for the full catalog, and `h3.md` for putting raster values on an H3 grid.

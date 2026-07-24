# Raster analytics (Phase 3, format-agnostic)

Runs on a `tile` column from **any** ingestion route — Phase 2b (standard read), 2c (retile-and-persist), or 2d (NetCDF/GRIB reduced to tiles). Nothing here is format-specific.

Each section explains a function: **what it does, when to use it, and which parameters are yours to choose.** Pick only what the workflow needs (none are required), and treat every literal value (CRS code, region, band layout) as an example. For H3, see `h3.md`.

```python
from databricks.labs.gbx.rasterx import functions as rx
import pyspark.sql.functions as F
rx.register(spark)
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

**What:** reduce each tile (per band) to a scalar. **When:** quick value ranges, QA, or building a stats table.

**Single-band** — stats as columns:

```python
tiles.select("source",
    rx.rst_avg("tile").alias("avg"),
    rx.rst_min("tile").alias("min"),
    rx.rst_max("tile").alias("max"),
    rx.rst_pixelcount("tile").alias("pixelcount"))
```

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
    .withColumn("avg",     rx.rst_avg("clipped"))
    .withColumn("bbox_wkb", rx.rst_boundingbox(F.col("clipped")))
    .select("source", F.expr("st_geomfromwkb(bbox_wkb)").alias("bbox"), "avg"))
```

## Zonal statistics (per region)

**What:** a statistic per polygon, across many regions. **When:** "mean/sum X per county/watershed/zone".

Two approaches: clip per polygon (cross raster tiles with the polygon set via `rst_clip`, then `rst_avg`/`rst_summary` grouped by a region key), or aggregate aligned tiles with the DataFrame aggregates (`rst_combineavg_agg`, `rst_merge_agg`) per group. Output is keyed by region → persist per **Phase 4**.

---

See `references/functions.md` for the full catalog, and `h3.md` for putting raster values on an H3 grid.

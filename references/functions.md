# RasterX Function Reference

GeoBrix has two execution tiers with the same `rst_*` API — **Lightweight** (`pyrx`) and **Heavyweight** (`rasterx`). See [execution tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers/) and `references/1-install/install-light.md` / `references/1-install/install-heavy.md`.

All RasterX functions operate on a `tile` column (raster type). In **Python** they're accessed as `rx.rst_*` after `rx.register(spark)`.

**Lightweight:** `register(spark)` registers `*_gbx` readers/writers; `rx.register(spark)` registers `rst_*` functions — both are required.

**Heavyweight:** the cluster JAR registers `gtiff_gdal` / `gdal`; call `rx.register(spark)` only.

```python
# Lightweight
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx import functions as rx
register(spark)
rx.register(spark)

# Heavyweight
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)
```

Authoritative API reference: https://databrickslabs.github.io/geobrix/docs/api/rasterx-functions — use this for canonical signatures.

## Metadata & Accessors

| Function | Purpose |
|---|---|
| `rst_boundingbox(tile)` | Spatial extent as a geometry |
| `rst_width(tile)`, `rst_height(tile)` | Pixel dimensions |
| `rst_numbands(tile)` | Band count |
| `rst_metadata(tile)` | Full metadata map |
| `rst_bandmetadata(tile, band)` | Per-band metadata |
| `rst_srid(tile)` | Coordinate reference system EPSG code |
| `rst_format(tile)` | Source format (e.g. `GTiff`) |
| `rst_getnodata(tile)` | NoData value |
| `rst_type(tile)` | Pixel data type |
| `rst_memsize(tile)` | Estimated memory footprint |
| `rst_pixelcount(tile)` | Total pixel count |
| `rst_summary(tile)` | Aggregated statistical summary |
| `rst_avg(tile)`, `rst_min`, `rst_max`, `rst_median` | Per-tile statistics |
| `rst_georeference(tile)` | Affine transform parameters |
| `rst_pixelwidth(tile)`, `rst_pixelheight(tile)` | Pixel size in CRS units |
| `rst_upperleftx(tile)`, `rst_upperlefty(tile)` | Origin coordinates |
| `rst_scalex(tile)`, `rst_scaley(tile)`, `rst_rotation`, `rst_skewx`, `rst_skewy` | Transform components |
| `rst_subdatasets(tile)` | List subdataset names (NetCDF/GRIB) |
| `rst_getsubdataset(tile, name)` | Extract a named subdataset |

## Constructors

| Function | Purpose |
|---|---|
| `rst_fromfile(path)` | Load raster from a file path |
| `rst_fromcontent(bytes)` | Construct from binary content |
| `rst_frombands([band_exprs])` | Compose from band expressions |
| `rst_tryopen(path)` | Safe open — returns NULL on failure |

## Transformations

| Function | Purpose |
|---|---|
| `rst_clip(tile, clip, cutlineAllTouched)` | Crop by geometry. **`clip` must be a WKT string or WKB binary** — do NOT pass `ST_GeomFromText(...)` output or UC `GEOMETRY` types. **`cutlineAllTouched`** is a boolean: `true` includes pixels the boundary touches, `false` only fully-inside pixels. Example: `rx.rst_clip("tile", F.lit(wkt_str), F.lit(True))`. |
| `rst_transform(tile, target_srid)` | Reproject to target CRS |
| `rst_merge(tile_a, tile_b)` | Combine two rasters |
| `rst_combineavg(tile_a, tile_b)` | Average aligned rasters |
| `rst_asformat(tile, fmt)` | Convert to target format (e.g. `COG`) |
| `rst_convolve(tile, kernel)` | Apply convolution filter |
| `rst_filter(tile, expr)` | Custom filter expression |
| `rst_mapalgebra(tile, expr)` | Map algebra (band math) |
| `rst_derivedband(tile, python_udf)` | Compute new band via Python UDF |
| `rst_ndvi(tile, red_band, nir_band)` | NDVI calculation |
| `rst_dtmfromgeoms(geoms, resolution)` | Rasterize geometries to a DTM |
| `rst_initnodata(tile, val)` | Set NoData value |
| `rst_updatetype(tile, type)` | Cast pixel data type |
| `rst_isempty(tile)` | Boolean — is the tile empty? |

## Coordinate conversion

| Function | Purpose |
|---|---|
| `rst_rastertoworldcoord(tile, px, py)` | Pixel → world coordinate |
| `rst_rastertoworldcoordx(...)`, `rst_rastertoworldcoordy(...)` | Component versions |
| `rst_worldtorastercoord(tile, x, y)` | World coord → pixel |
| `rst_worldtorastercoordx(...)`, `rst_worldtorastercoordy(...)` | Component versions |

## Generators (tiling / tessellation)

| Function | Purpose |
|---|---|
| `rst_separatebands(tile)` | Explode multi-band tile into one row per band |
| `rst_retile(tile, size)` | Retile to a specified pixel size |
| `rst_maketiles(extent, grid)` | Build tiles from a grid definition |
| `rst_tooverlappingtiles(tile, width, height, overlap)` | Tile with overlap (for edge-aware ops, and to give a coarse grid enough cells before H3). Four args: e.g. `rx.rst_tooverlappingtiles("tile", F.lit(32), F.lit(32), F.lit(0))`. |
| `rst_h3_tessellate(tile, resolution)` | Tessellate raster into H3 cells |

> ### ⚠️ Generators (fan-out): pick ONE surface and keep every example consistent on it
>
> GeoBrix exposes two call surfaces — **Python column API** (`rx.rst_*`) and **SQL** (`gbx_rst_*`).
> Both are valid. **The rule for this skill's examples: choose one surface and stay on it for the
> WHOLE workflow — never mix them within an example.** Default the examples to the **Python column
> API** (it's what we've tested, and it doesn't depend on SQL registration).
>
> **Default (Python column API) — TESTED on Heavy (VIIRS notebook):**
> ```python
> retiled_df = raster_df.withColumn("retiled", rx.rst_retile("tile", F.lit(4096), F.lit(4096)))
> retiled_df.write.mode("overwrite").saveAsTable(retiled_table)   # returns an ARRAY<tile> column; persists directly
> ```
> No `explode`, no `LATERAL`. `explode()` only if you want one row per tile downstream.
>
> **SQL alternative (offer to the user if they prefer SQL) — then swap the ENTIRE workflow to SQL,
> not just one step.** SQL uses the `gbx_`-prefixed name and the `LATERAL` form for generators:
> ```sql
> -- Python  rx.rst_retile(tile, w, h)   ⇄   SQL  LATERAL gbx_rst_retile(tile, w, h)
> SELECT s.source, t.* FROM src s, LATERAL gbx_rst_retile(s.tile, 4096, 4096) t
> ```
> Python↔SQL name mapping: `rx.<name>(...)` ⇄ `gbx_<name>(...)`. Full catalog above / at
> https://databrickslabs.github.io/geobrix/docs/api/raster-functions.
> **Prerequisite for the SQL surface:** the `gbx_rst_*` name must be registered in the session via
> `register(spark)`. On our cluster this did NOT resolve (`gbx_rst_retile` →
> `UNRESOLVABLE_TABLE_VALUED_FUNCTION`) — so if a user chooses SQL, first confirm the name resolves
> (`spark.sql("SHOW FUNCTIONS").filter("function LIKE '%rst_%'")`) before building the SQL workflow.
>
> **What's verified vs. not:**
>
> | | Python `rx.rst_*` | SQL `gbx_rst_*` |
> |---|---|---|
> | **Heavy** | ✅ tested (`withColumn`, array column) | exists (JAR-registered); our call didn't resolve — confirm registration before use |
> | **Light** | generators **raise `NotImplementedError`** (observed) → use the SQL surface | needs a successful `register(spark)`; unverified here (see `todo.md` Part 8) |
>
> **Notes:**
> - **Consistency is the rule, not Python-vs-SQL.** Either surface is fine; just don't half-swap an example.
> - Non-generator functions (stats, clip, transform, NDVI…) are plain column/SQL expressions on both tiers — this note is only about the fan-out generators (`rst_retile`, `rst_maketiles`, `rst_tooverlappingtiles`, `rst_separatebands`, `rst_h3_tessellate`, `rst_polygonize`).
> - **Tested for `rst_retile` only.** Other generators share the same base but their exact form is not verified here — confirm before relying on it.

## H3 grid aggregation

Aggregate raster pixel values to H3 hex cells:

| Function | Purpose |
|---|---|
| `rst_h3_rastertogridavg(tile, resolution)` | Mean per H3 cell |
| `rst_h3_rastertogridcount(tile, resolution)` | Pixel count per cell |
| `rst_h3_rastertogridmax(...)`, `rst_h3_rastertogridmin(...)`, `rst_h3_rastertogridmedian(...)` | Per-cell stats |

Output is typically `(h3_cell, value)` rows ready to join with downstream tables.

## DataFrame aggregates

For aggregating across rows (e.g., per polygon, per region):

| Function | Purpose |
|---|---|
| `rst_combineavg_agg(tile)` | Aggregated average across rows |
| `rst_merge_agg(tile)` | Aggregated merge across rows |
| `rst_derivedband_agg(tile, udf)` | Aggregated band derivation |

## Reader options

For `spark.read.format("gdal")` or named readers (`gtiff_gdal`, etc.):

| Option | Default | Purpose |
|---|---|---|
| `driverName` | inferred from extension | Force GDAL driver. Use GDAL's canonical short names: `GTiff`, `netCDF` (lowercase `n`/`C` — not `NetCDF`), `GRIB` |
| `sizeInMB` | `16` | Split files over this threshold during read |
| `filterRegex` | `.*` | Filter input paths by regex |

## Reader naming

| Named reader | Underlying driver | Use for |
|---|---|---|
| `gtiff_gdal` | GTiff | GeoTIFF and BigTIFF |
| `gdal` | inferred | Any GDAL-supported raster format |
| `shapefile_ogr` | ESRI Shapefile | Vector — for joining with rasters |
| `geojson_ogr` | GeoJSON / GeoJSONSeq | Vector |
| `gpkg_ogr` | GeoPackage | Vector |
| `file_gdb_ogr` | OpenFileGDB | Vector — ESRI file geodatabases |

## Notes

- Functions are also callable directly in Spark SQL, but the prefix is version-dependent (`gbx_rst_*` per docs, `rst_*` on some installs). Verify with `spark.sql("SHOW FUNCTIONS").filter("function LIKE '%rst_%'")` (the bare `SHOW FUNCTIONS LIKE '*rst_*'` form throws a SQL parse error on some Serverless / Spark Connect runtimes — filter the result instead), then call e.g. `SELECT <prefix>rst_width(tile) FROM rasters`
- After ingestion via vector readers (`*_ogr`), output uses `geom_0` / `geom_0_srid` / `geom_0_srid_proj` columns (or `shape*` / `SHAPE*` depending on the reader). Convert to native UC `GEOMETRY`/`GEOGRAPHY` types as a downstream step if needed
- For functions not listed here, check the latest API at https://databrickslabs.github.io/geobrix/docs/packages/rasterx

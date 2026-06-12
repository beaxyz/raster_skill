# NetCDF / GRIB ingestion (subdataset formats)

**This is the `ROUTING: SUBDATASET` destination from Phase 2a/2d.** NetCDF, GRIB, and HDF expose multiple **variables as subdatasets** and are usually multidimensional (`time × level × lat × lon`). Their CRS may be carried in CF metadata (GDAL often infers it) or absent — check `rst_srid` rather than assuming. The byte-based size gate does **not** apply — a `.nc` is often a *coarse* spatial grid over many timesteps, so its "size" is its logical shape, not GB on disk.

> ⚠️ **GeoBrix officially supports GeoTIFF only.** NetCDF/GRIB are best-effort via the generic GDAL driver. Because of that, using **xarray** for the reshape/rechunk step is a legitimate, expected exception to the substitution policy (GeoBrix still does all the raster analytics). Surface it to the user: *"Using xarray to subset/rechunk the NetCDF; GeoBrix does the raster analysis."*
>
> **Driver name is `netCDF`** (GDAL's canonical short name) — not `NetCDF`.

This example follows two scenarios **in sequence**:
- **Scenario A** — one big `.nc` → rechunk by a variable into smaller files (xarray). Its output is the input to Scenario B.
- **Scenario B** — already-chunked `.nc` files → analyse with GeoBrix.

If Phase 2a reported a single large file, do A then B. If it already reported many chunked files, skip to B.

---

## Step 1 — Inspect: what variables, what dimensions, chunked or not?

Phase 2a already told you it's a subdataset format and how many files there are. Now peek the **logical** structure to decide the branch and (for Scenario A) which variable to chunk on.

```python
%pip install xarray netcdf4
dbutils.library.restartPython()
```

```python
import xarray as xr

netcdf_file = "${source_path}"            # the single .nc (Scenario A) — a directory? you're already chunked → Scenario B
ds = xr.open_dataset(netcdf_file)
print(ds)                                  # dims (time/level/lat/lon), coords, CRS hints
print("Variables:", list(ds.data_vars))    # candidate variables to chunk/analyse on
```

`print(ds)` shows the dimensions and coordinate variables (e.g. `valid_time`, `latitude`, `longitude`) and the data variables. The number of **timesteps × variables**, not the file size, is what determines how heavy this is.

---

## Scenario A — one big `.nc` → rechunk by a variable

ECMWF ERA5 monthly file used here. The big file holds several variables, each `time × lat × lon`; we reduce it to **one variable, split into daily files**.

**1. List variables and confirm which one with the user.** Don't hardcode — present the list and ask.

```python
list(ds.data_vars)        # e.g. ['t2m', 'd2m', 'sp', ...]  → ask the user which one
target_var = "${variable}"  # confirmed with user (this notebook used 'd2m' = 2m dewpoint)
```

**2. Rechunk by day and write per-day NetCDF files.** Group the chosen variable by the date part of its time coordinate, write each group to a `.nc`.

```python
import os, shutil

netcdf_chunked = "${chunked_dir}"          # e.g. /Volumes/<cat>/<schema>/netcdf/raw_chunked/
local_tmp = "/tmp/era5_daily/"
os.makedirs(local_tmp, exist_ok=True)
os.makedirs(netcdf_chunked, exist_ok=True)

dates = ds.valid_time.dt.date              # the time coordinate's date; adjust name to your file's coord
for date, group in ds[target_var].groupby(dates):
    local_path  = f"{local_tmp}{date}.nc"
    volume_path = f"{netcdf_chunked}{date}.nc"
    group.to_netcdf(local_path)            # write locally first…
    shutil.copy(local_path, volume_path)   # …then copy to the Volume
```

> **Why local-tmp-then-copy:** `to_netcdf` (netcdf4/HDF5) needs random-access writes that the `/Volumes` FUSE mount doesn't support. Write to `/tmp`, then `shutil.copy` the finished file onto the Volume.

The chunking key here is **day**; pick whatever axis carries the volume (day, month, level…). Output is a directory of chunked `.nc` files → continue to **Scenario B**.

---

## Scenario B — process chunked NetCDF with GeoBrix

```python
from databricks.labs.gbx.rasterx import functions as rx
import pyspark.sql.functions as F
rx.register(spark)

netcdf = (spark.read.format("gdal")
                    .option("driverName", "netCDF")   # canonical GDAL short name
                    .load("${chunked_dir}"))
netcdf.count()
netcdf.display()
```

**Each band is a timestep, not a spectral band** (in this example, an hour — the daily files carry 24 hourly bands). Whatever the cadence, you `posexplode` *all* bands and map `band_index → timestamp` per your file's time layout — you do **not** index `[0]` the way you would for a single-band GeoTIFF.

### Check the CRS first — don't assume it's missing, don't hardcode it

GDAL usually **infers** the CRS from the NetCDF CF metadata (the `latitude`/`longitude` coordinate variables) when it materializes each subdataset to a GTiff tile, so `rst_srid("tile")` is often already a valid code — not `0`. **Check before doing anything:**

```python
netcdf.select(rx.rst_srid("tile").alias("srid")).distinct().show()
```

- **Valid SRID returned** → nothing to fix. Apply `rx.rst_transform("tile", <epsg>)` only when you need to *match a different CRS* — e.g. align the raster to your boundary table before clipping. The notebook transforms to `4326` because the `gold_cities` boundaries are in 4326:
  ```python
  .withColumn("srid_tile", rx.rst_transform("tile", 4326))   # align raster CRS to the 4326 boundary
  ```
- **`rst_srid` returns `0` (genuinely missing)** → do **not** hardcode a guess. **Ask the user** which CRS the grid is in. CF lat/lon is usually EPSG:4326, but rotated-pole / projected grids are not — only the data owner knows. Once they confirm the EPSG, apply it with `rx.rst_transform("tile", <confirmed_epsg>)`.

### Analytics → use the format-agnostic docs

Once Scenario B lands a `tile`/`srid_tile` column, the analytics are **the same as for any ingested raster** — they're documented format-agnostically, not here:

- **Stats & clip & zonal** → `references/examples/raster-analytics.md`
- **H3 (tessellate / aggregate / time-series)** → `references/examples/h3-examples.md`

**The one NetCDF thing to carry forward:** the bands are **timesteps**, so use the **multi-temporal** variants in those docs — `posexplode` *all* bands and map `band_index → timestamp`; never index `[0]`. *How* you map `band_index` to a real time depends on how your files lay out time (one file per day with hourly bands, one file per timestep, a time coordinate variable, …) — there's no fixed formula. If, for example, each file is a day with hourly bands and the date is in the filename:

```python
# EXAMPLE mapping — adapt to your files' actual time layout
.withColumn("date", F.to_date(F.regexp_extract("source", r"(\d{4}-\d{2}-\d{2})", 1)))   # date encoded in filename
# …after posexplode(...) as ("band_index", …):
.withColumn("timestamp", F.expr("timestampadd(HOUR, band_index, cast(date as timestamp))"))  # bands = hours
```

See the temporal-stack and pre-retile sections of `h3-examples.md` for the generalized H3 time-series, and `raster-analytics.md` for clip/stats.

---

## Re-enter the main flow

Scenario B lands a `tile`/`srid_tile` column (or exploded H3 rows) — that's **Phase 3** territory in `SKILL.md`. Persist analytic outputs per **Phase 4**.

## NetCDF-specific gotchas (summary)

| Gotcha | Handling |
|---|---|
| Driver name | `driverName="netCDF"` (not `NetCDF`) |
| Multiple variables | `list(ds.data_vars)` → **ask the user** which one before chunking |
| Bands = timesteps, not spectral | `posexplode` all bands + `band_index → timestamp`; don't index `[0]` |
| CRS | Check `rst_srid("tile")` first — often inferred from CF metadata. Align to your boundary's CRS via `rst_transform`. If genuinely `0`, **ask the user** for the EPSG — don't hardcode |
| `to_netcdf` fails on `/Volumes` | write to `/tmp` then `shutil.copy` to the Volume |
| Coarse grid → too few H3 cells | `rst_tooverlappingtiles` (sub-tile size is your choice) before `rst_h3_rastertogridavg` — see `h3-examples.md` |
| `rst_clip` geometry | pass a **WKT string** (`F.lit(wkt)`), not a UC `GEOMETRY` |

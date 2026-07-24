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
| **HIGH** | Zarr | dir | **Very popular** cloud-native array store; growing fast for large EO/climate archives. | Not started — GDAL claims support, no repo fixtures. |
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

### 1. Light tier in example docs (MED)
Phase 3 `rst_*` analytics are tier-agnostic, but example docs and the Phase 2b/3 snippets
still hardcode Heavy (`rasterx`, `gtiff_gdal`/`gdal`). Update:
- `3-process/analytics.md`, `3-process/h3.md`, `2-ingest/2c-large-raster-retile.md` → use the Phase 1 bootstrap pattern: `register(spark)` + `GEOTIFF_READER`/`GENERIC_READER` for Light; `rasterx` + `gtiff_gdal`/`gdal` for Heavy.
- Align `SKILL.md` Phase 2b with `GEOTIFF_READER` from Phase 1.
- Leave `2-ingest/2d-netcdf-grib.md` heavy-only until the NetCDF light path is defined (item 2).

### 2. NetCDF/GRIB tier routing + light path (MED)
Define upfront tier routing for NetCDF/GRIB and the xarray→GeoTIFF→light path. Until then,
`2-ingest/2d-netcdf-grib.md` stays heavy-only.

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

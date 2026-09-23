# GeoBrix Lightweight Install

Install GeoBrix **Lightweight** tier (`pyrx`) — a single wheel with no JAR, no GDAL init script, and no native `.so`. Runs on **Serverless** (preferred), classic shared/ARM, and classic dedicated clusters.

Source: [GeoBrix execution tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers/) and [GitHub quick start](https://github.com/databrickslabs/geobrix#quick-start-lightweight).

For **Heavyweight** (JAR + init script on classic x86), see [`references/1-install/install-heavy.md`](1-install/install-heavy.md).

## When to use this path

- Default for most raster tasks (GeoTIFF ingest, NDVI, clip, H3, zonal stats)
- **Preferred compute: Serverless** — use the Connect dropdown to attach Serverless if available
- Fallback: classic cluster when Serverless is not enabled or not listed in Connect

Route to **Heavyweight** instead when the task needs OGR readers (`*_ogr`), exotic `gdal` driver options, PMTiles writer, or `conforming` GridX/VectorX triangulation. See `SKILL.md` → Execution tier selection.

## Prerequisites

| Requirement | Lightweight |
|---|---|
| DBR | **17.3 LTS or 18 LTS** (per GeoBrix supported runtimes) |
| Python | **3.12** on supported DBR (3.11 minimum for `[light]` deps) |
| Serverless | Environment **5+** (Python 3.12) if on Serverless |
| UC Volume | WHL staging only — no JAR, `.so`, or init script |
| Cluster permissions | `%pip` in notebook; optional cluster WHL on classic |

**Version caveat:** Some releases (e.g. 0.3) may not ship `pyrx` yet. Run the version gate below; if import fails, use Heavyweight on classic or upgrade GeoBrix.

## Parameters

Same propose-then-confirm pattern as `1-install/install-heavy.md`. Lightweight only needs the WHL in a Volume.

### Ask the user (propose-then-confirm)

| Parameter | Description |
|---|---|
| `volume_path` | UC Volume for the WHL. Propose `/Volumes/<catalog>/<schema>/geobrix_artifacts` from context. Lightweight does **not** need JAR or init script paths. |

### Resolve automatically — do NOT ask the user

| Parameter | How to resolve | Default if resolution fails |
|---|---|---|
| `geobrix_version` | Latest tag from https://github.com/databrickslabs/geobrix/releases/latest | `0.2.0` |
| `workspace_profile` | `DEFAULT` unless user is on another Databricks CLI profile | `DEFAULT` |

## Pre-flight checks

Run from a notebook before `%pip` install.

### Check 1: Compute type + Serverless-first recommendation

```python
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
compute = "serverless" if not cluster_id else "classic"

if compute == "serverless":
    print("✅ On Serverless — preferred compute for Lightweight GeoBrix.")
else:
    print("ℹ️  On classic cluster.")
    print("   Serverless is preferred for Lightweight install when available.")
    print("   Check the Connect dropdown — switch to Serverless if listed.")
    print("   Continuing on classic is OK if Serverless is unavailable.")
```

### Check 2: DBR and Python version

```python
import re
import sys

ver = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "")
m = re.match(r"(\d+)\.(\d+)", ver)
py = sys.version_info

if m and (int(m.group(1)), int(m.group(2))) >= (17, 3):
    print(f"✅ DBR version OK: {ver}")
else:
    print(f"❌ DBR too old for Lightweight: {ver}. Need 17.3+ LTS or 18 LTS.")

if py >= (3, 11):
    print(f"✅ Python OK: {sys.version.split()[0]}")
else:
    print(f"❌ Python {sys.version.split()[0]} — Lightweight needs 3.11+.")
    print("   On Serverless: upgrade to environment 5+ (Python 3.12).")
```

### Check 3: UC Volume reachable

Same as `1-install/install-heavy.md` Check 3 — confirm `volume_path` exists and is writable.

### Check 4: Version availability gate

```python
try:
    import databricks.labs.gbx.pyrx  # noqa: F401
    print("✅ pyrx module present in environment (or will be after %pip).")
except ImportError:
    print("⚠️  pyrx not installed yet — normal before %pip.")
    print("   After install, if import still fails, this GeoBrix release may not ship Lightweight.")
    print("   Fall back to Heavyweight on classic x86 (see 1-install/install-heavy.md).")
```

## Step 1: Download the wheel

From [GeoBrix Releases](https://github.com/databrickslabs/geobrix/releases), download:

| File | Purpose |
|---|---|
| `geobrix-<version>-py3-none-any.whl` | Python bindings + Lightweight tier (`pyrx`, `[light]` extra deps) |

No JAR or `libgdalalljni.so` needed.

## Step 2: Upload to UC Volume

From a local terminal:

```bash
databricks fs cp geobrix-${geobrix_version}-py3-none-any.whl \
  ${volume_path}/ --profile ${workspace_profile}
```

## Step 3: Install in the notebook

Use the **PEP 508 quoted form** — required on Serverless (`%pip` keeps quotes; putting `[light]` on the path breaks pip):

```python
%pip install "geobrix[light] @ file://${volume_path}/geobrix-${geobrix_version}-py3-none-any.whl"
dbutils.library.restartPython()
```

On classic clusters you may alternatively attach the WHL via the cluster **Libraries** tab for persistence across sessions.

## Step 4: Register readers and functions

Lightweight needs two registrations (same as [GeoBrix quick start](https://github.com/databrickslabs/geobrix#quick-start-lightweight)): `register(spark)` for `*_gbx` readers/writers, then `rx.register(spark)` for `rst_*` analytics.

```python
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx import functions as rx

register(spark)
rx.register(spark)
```

Lightweight readers use the `*_gbx` suffix: `gtiff_gbx`, `raster_gbx`. Heavyweight uses `gtiff_gdal`, `gdal` — see execution tiers docs.

## Step 5: Verify

```python
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx import functions as rx

register(spark)
rx.register(spark)

api_names = sorted(n for n in dir(rx) if not n.startswith("_"))
rst_fns = [n for n in api_names if n.startswith("rst_")]

print(f"✅ Lightweight GeoBrix: {len(rst_fns)} rst_* on Python API")
print(f"   Tier: light | Readers: gtiff_gbx, raster_gbx")

# Optional SQL-registration count. `SHOW FUNCTIONS` can throw a SQL parse error on some
# Serverless / Spark Connect runtimes, so keep it non-fatal — the Python API count above
# is the real confirmation that GeoBrix registered.
try:
    n_sql = spark.sql("SHOW FUNCTIONS").filter("function LIKE '%rst_%'").count()
    print(f"   SQL functions (matching rst_): {n_sql}")
except Exception as e:
    print(f"   (SQL function count skipped — SHOW FUNCTIONS not available here: {type(e).__name__})")
```

If `ImportError` on `pyrx` after `%pip` and restart → this release likely lacks Lightweight. Use Heavyweight (`1-install/install-heavy.md`) on classic x86 or upgrade GeoBrix.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Expected package name at the start of dependency specifier` | `[light]` on file path instead of PEP 508 form | Use `"geobrix[light] @ file:///Volumes/.../geobrix-...whl"` |
| `ImportError: databricks.labs.gbx.pyrx` after install | Release lacks Lightweight tier | Heavyweight on classic, or upgrade GeoBrix |
| Python 3.10 on Serverless | Environment < 5 | Upgrade Serverless environment to 5+ or use classic 17.3+ |
| `Driver not found` on `gtiff_gbx` | `register(spark)` not called for `*_gbx` readers | Run Step 4 before reading |
| SQL functions empty but `dir(rx)` OK | `rx.register(spark)` not run | Re-run registration |

## Serverless notes

- Install is `%pip` per session (unless cluster library on classic)
- No init script or cluster library JAR
- Serverless environment **5+** required for Python 3.12
- Heavyweight tier cannot run on Serverless — switch to classic x86 if heavy-only features are needed

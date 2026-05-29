# GeoBrix Cluster Setup

Complete steps to install GeoBrix on a Databricks classic cluster.

## Prerequisites

- **DBR 17.1 or later** (LTS preferred). Older runtimes lack native spatial types GeoBrix interoperates with.
- **Classic compute** — Serverless clusters are NOT supported. GeoBrix needs cluster-level init scripts and JAR installation.
- **UC Volume** with write access — hosts the JAR, `.so`, init script, and WHL artifacts.
- **Cluster permissions** to attach init scripts and upload libraries (`CAN MANAGE` on the cluster).

## Parameters

Some parameters need to come from the user; others Claude should resolve on its own without asking.

### Ask the user (propose-then-confirm)

| Parameter | Description |
|---|---|
| `volume_path` | Full UC path to the Volume that will host artifacts. **Do not just ask "what's your Volume path?"** — propose a concrete default based on context, then ask the user to confirm or override. See below. |

**How to propose `volume_path`:**

1. Look at the user's recent context for any Volume / catalog / schema they've already mentioned (e.g., a TIFF path like `/Volumes/beatrice_liew/geospatial/...` implies catalog `beatrice_liew`, schema `geospatial`).
2. Propose a sub-Volume under the same catalog/schema with a clear name: `/Volumes/<catalog>/<schema>/geobrix_artifacts`.
3. If no context is available, fall back to `/Volumes/main/geobrix/artifacts` and offer to adjust.
4. Phrase as a confirmation, not an open question:
   > *"I'll use `/Volumes/<catalog>/<schema>/geobrix_artifacts` for the GeoBrix JAR, .so, init script, and WHL. If you'd prefer a different Volume, let me know. Should I also create it now if it doesn't exist?"*
5. **Offer to create the Volume** (`CREATE VOLUME IF NOT EXISTS <cat>.<schema>.geobrix_artifacts`) and grant `READ VOLUME` / `WRITE VOLUME` if the path the user confirms doesn't exist yet.

### Resolve automatically — do NOT ask the user

| Parameter | How to resolve | Default if resolution fails |
|---|---|---|
| `geobrix_version` | **Always use the latest stable release.** Fetch the latest tag from https://github.com/databrickslabs/geobrix/releases/latest (or `gh api repos/databrickslabs/geobrix/releases/latest --jq .tag_name`). Do NOT ask the user about version unless they explicitly raise it. | `0.2.0` (known-good at time of skill writing) |
| `cluster_id` | Read from `spark.conf.get("spark.databricks.clusterUsageTags.clusterId")` on the attached cluster. Only needed for SDK automation; skip if doing manual UI setup. | N/A — must derive from session |
| `workspace_profile` | Default to `DEFAULT` unless the user is on a non-default profile (check via `databricks auth profiles`). Only relevant when running CLI commands from a local terminal, not from a notebook. | `DEFAULT` |

Once `volume_path` is collected, substitute all four values into every code block / CLI command below.

## Pre-flight checks

Before downloading or uploading anything, run these checks from a notebook attached to the target cluster. All must pass before proceeding to Step 1.

### Check 1: Compute type is classic, plus access mode (Dedicated strongly preferred)

This check is **safe to run on both Dedicated and Shared/Standard access modes** — it does NOT use `spark.sparkContext` (which is restricted on Shared mode). Do not introduce `spark.sparkContext.master` here; use only `spark.conf.get(...)` which works on every access mode.

```python
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
cluster_profile = spark.conf.get("spark.databricks.cluster.profile", "")

# Map raw profile values to human-readable access modes
mode_map = {
    "singleNode": "Single Node",
    "singleUser": "Dedicated (Single User)",
    "shared": "Standard (Shared)",
    "": "Unknown / legacy",
}
access_mode = mode_map.get(cluster_profile, cluster_profile)

if not cluster_id:
    print("❌ Not on a classic cluster. GeoBrix requires classic All-Purpose / Job compute.")
    print("   Fix: click the Connect dropdown at the top right of the notebook and pick a classic cluster.")
else:
    print(f"✅ Attached to a classic cluster")
    print(f"   Cluster ID:  {cluster_id}")
    print(f"   Access mode: {access_mode}")

    if cluster_profile == "singleUser":
        print("   ℹ️  Dedicated mode — the recommended path for GeoBrix. Proceed.")
    elif cluster_profile in ("shared",):
        print()
        print("   ⚠️  SHARED ACCESS MODE DETECTED — STOP AND ASK BEFORE PROCEEDING.")
        print("   On Shared mode the UC Artifact Allowlist is enforced. This means:")
        print("     • The GeoBrix init script path must be on the metastore INIT_SCRIPT allowlist")
        print("     • The GeoBrix JAR path must be on the metastore LIBRARY_JAR allowlist")
        print("     • Adding to the allowlist requires metastore admin permission")
        print()
        print("   On Dedicated (Single User) mode allowlist enforcement is typically not required,")
        print("   so install is faster and less permission-bound.")
    elif cluster_profile == "singleNode":
        print("   ℹ️  Single Node — fine for development. Treat as Dedicated for install purposes.")
```

### If on Shared mode: STOP and ask the user which path

**Do not proceed with install steps until the user picks one of these.** Surface the three options as a clear question and wait for their answer:

> *"You're on a Shared access mode cluster. GeoBrix can install here, but it's significantly easier on Dedicated mode (no UC artifact allowlist requirement). Pick one:*
>
> *(a) **Use a different cluster that's Dedicated** — I'll help you switch to it via the Connect dropdown.*
> *(b) **Change THIS cluster from Shared → Dedicated** — requires CAN MANAGE permission on the cluster; I can run the SDK update if you confirm.*
> *(c) **Stay on Shared** — requires metastore admin permission to add the GeoBrix paths to the artifact allowlist. If you have that (or know who to ask), I can guide you through it.*
>
> *Which path do you want to take?"*

### Helper: changing an existing cluster from Shared → Dedicated (Option b)

Only run this after the user confirms (b). Requires CAN MANAGE on the cluster.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import DataSecurityMode, ClusterAttributes

w = WorkspaceClient()
cluster_id = "${cluster_id}"
user_name = w.current_user.me().user_name  # or hardcode the user who'll own the cluster

# Use clusters.update() (partial update) — NOT clusters.edit(), which is full-replace
w.clusters.update(
    cluster_id=cluster_id,
    update_mask="data_security_mode,single_user_name",
    cluster=ClusterAttributes(
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=user_name,
    ),
)
print(f"✅ Changed access mode to Dedicated; cluster owner = {user_name}. Restart required.")
w.clusters.restart_and_wait(cluster_id=cluster_id)
```

### Helper: allowlisting GeoBrix paths (Option c)

Only run this after the user confirms (c) and confirms they have metastore admin. See the "Adding artifacts to the UC allowlist" section below for the full SDK pattern (fetch → append → update, to avoid wiping existing entries).

### Why surface access mode at all

- It drives the most consequential decision in this install (Dedicated vs Shared).
- If install completes but `rx.register(spark)` fails later, the access mode is the first thing to check.
- The Shared-mode allowlist friction has bitten multiple users — naming it upfront avoids a half-finished install.

**If Check 1 fails (no `cluster_id`):** click the **Connect** dropdown at the top right of the notebook and pick a classic cluster (not Serverless, not a SQL warehouse). The remaining checks won't be meaningful until this passes.

### Check 2: DBR version

```python
import re

ver = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "")
m = re.match(r"(\d+)\.(\d+)", ver)
if m and (int(m.group(1)), int(m.group(2))) >= (17, 1):
    print(f"✅ DBR version OK: {ver}")
else:
    print(f"❌ DBR version too old: {ver}. Need 17.1+. Switch the cluster's Databricks Runtime in cluster settings.")
```

**If fails:** open the cluster page → Edit → set Databricks Runtime to 17.1 LTS or newer → restart.

### Check 3: Unity Catalog is enabled and a Volume is reachable

Replace the path below with the Volume you intend to use:

```python
volume_path = "/Volumes/main/geobrix/artifacts"  # ← edit me

try:
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    if len(catalogs) <= 1 and catalogs[0] == "hive_metastore":
        print(f"❌ UC not enabled — only catalogs found: {catalogs}")
    else:
        print(f"✅ UC enabled. Catalogs visible: {len(catalogs)}")

    # Confirm Volume path exists / is writable
    dbutils.fs.ls(volume_path)
    print(f"✅ Volume reachable: {volume_path}")
except Exception as e:
    print(f"❌ Volume check failed for {volume_path}: {e}")
    print("   Fix: create the Volume (CREATE VOLUME <cat>.<schema>.<name>) and grant READ VOLUME + WRITE VOLUME to your user.")
```

**If fails:**
- UC not enabled → ask a workspace admin to enable Unity Catalog
- Volume missing → `CREATE VOLUME main.geobrix.artifacts` (adjust catalog/schema) and grant `READ VOLUME`, `WRITE VOLUME` to the relevant principal

### Check 4: Cluster library permission (manual)

There's no notebook-level API to check this. Verify via the UI:

1. Open the cluster page → **Permissions** tab
2. Confirm your user (or a group you belong to) has **CAN MANAGE**
3. If not, request access from a cluster owner or workspace admin

Without `CAN MANAGE` you can't add init scripts or install the WHL → Steps 4 and 5 will fail.

---

Once **all four checks pass**, continue to Step 1.

## Adding artifacts to the UC allowlist (Shared-mode-only requirement)

**Only needed if** the user is staying on Shared access mode (Check 1 Option c). On Dedicated mode this is usually not required and you can skip directly to Step 1.

Workspaces with stricter UC governance enforce an **artifact allowlist** at the metastore level. Init scripts and JARs on UC Volumes must be approved before clusters can use them. The error you'll see if not allowlisted:

```
INVALID_PARAMETER_VALUE: Attempting to install the following init scripts that are not in the allowlist.
/Volumes/.../geobrix-gdal-init.sh: PERMISSION_DENIED: '...' is not in the artifact allowlist
```

**Required permission:** metastore admin. If you don't have it, ask the metastore owner.

### Safe pattern: fetch → append → update

`artifact_allowlists.update()` is full-replace at the artifact-type level — calling it with only your entry will wipe everything else. **Always fetch existing entries first and append.**

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ArtifactType, ArtifactMatcher, MatchType

w = WorkspaceClient()
volume_prefix = "${volume_path}/"  # trailing slash — allowlists the whole directory

def allowlist_prefix(artifact_type, prefix):
    current = w.artifact_allowlists.get(artifact_type=artifact_type)
    matchers = list(current.artifact_matchers or [])
    already_present = any(m.artifact == prefix for m in matchers)
    if already_present:
        print(f"✅ {artifact_type} already allowlists {prefix}")
        return
    matchers.append(ArtifactMatcher(artifact=prefix, match_type=MatchType.PREFIX_MATCH))
    w.artifact_allowlists.update(artifact_type=artifact_type, artifact_matchers=matchers)
    print(f"✅ Added to {artifact_type} allowlist: {prefix}")

# Both INIT_SCRIPT (for the .sh) and LIBRARY_JAR (for the .jar) need to be allowlisted
allowlist_prefix(ArtifactType.INIT_SCRIPT, volume_prefix)
allowlist_prefix(ArtifactType.LIBRARY_JAR, volume_prefix)
```

`PREFIX_MATCH` against the directory means **all files under it** are allowed — you don't need separate entries for the init script and the JAR.

### UI alternative

Catalog Explorer → click the metastore name (top of the catalog list) → **Settings** → **Artifact allowlist** → add a `PREFIX_MATCH` entry for `${volume_path}/` under both `INIT_SCRIPT` and `LIBRARY_JAR`.

## Step 1: Download artifacts

From the [GeoBrix Releases page](https://github.com/databrickslabs/geobrix/releases), grab the latest release of:

| File | Purpose |
|---|---|
| `geobrix-<version>-jar-with-dependencies.jar` | The Scala/Java implementation (Spark JAR) |
| `libgdalalljni.so` | GDAL native shared object — required for JNI calls into GDAL |
| `geobrix-<version>-py3-none-any.whl` | Python bindings — registers `gbx_rst_*` SQL functions and PySpark API |

## Step 2: Upload from local to a UC Volume

From a **local terminal** (where Step 1 downloaded the artifacts), push them up to the UC Volume. The destination path is `volume_path` from the parameters table; substitute the values the user provided.

```bash
# From the directory containing the downloaded artifacts:
databricks fs cp geobrix-${geobrix_version}-jar-with-dependencies.jar \
  ${volume_path}/ --profile ${workspace_profile}

databricks fs cp libgdalalljni.so \
  ${volume_path}/ --profile ${workspace_profile}

databricks fs cp geobrix-${geobrix_version}-py3-none-any.whl \
  ${volume_path}/ --profile ${workspace_profile}
```

**Source side** (left arg, no prefix) = your local working directory.
**Destination side** (right arg, `/Volumes/...`) = the UC Volume in the workspace.

If you see `dbfs:/Volumes/...` in older docs, it's the legacy syntax — works, but the modern form is just `/Volumes/...`.

## Step 3: Stage the init script

Download `geobrix-gdal-init.sh` from the [GeoBrix repo scripts directory](https://github.com/databrickslabs/geobrix/tree/main/scripts).

**Edit one line**: change `VOL_DIR` to the user's `volume_path` (the upstream default points to GeoBrix's example Volume, not yours).

```bash
# Before (upstream default):
VOL_DIR="/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix"

# After (substitute volume_path from parameters):
VOL_DIR="${volume_path}"
```

Why only this line: the script copies the JAR via a wildcard (`$VOL_DIR/geobrix-*-jar-with-dependencies.jar`) so any version uploaded into `VOL_DIR` is picked up automatically. The `.so` filename is exact (`libgdalalljni.so`) — works as-is if you didn't rename the downloaded file.

The script then:
1. Adds Ubuntu repos + GIS PPA, installs GDAL system dependencies (`libgdal-dev`, `gdal-bin`, `python3-gdal`)
2. Installs Python GDAL bindings via `pip` (matched to installed GDAL version)
3. Copies `libgdalalljni.so` from `VOL_DIR` to `/usr/lib/`
4. Copies the JAR (via wildcard) from `VOL_DIR` to `/databricks/jars/`

Upload the edited init script to the same Volume:

```bash
databricks fs cp geobrix-gdal-init.sh \
  ${volume_path}/ --profile ${workspace_profile}
```

## Step 4: Configure the cluster

Two paths — pick whichever fits the user's context:

| Path | When |
|---|---|
| **4a. Manual UI** (below) | One-off setup, walking a customer through it, no automation needed |
| **4b. SDK automation** (see "Automating with Databricks SDK" section below) | Repeatable setup, multiple clusters, infra-as-code |

### 4a. Manual UI path

In the cluster Edit page → Advanced Options:

**Init Scripts tab**
- Source: Volume
- Path: `${volume_path}/geobrix-gdal-init.sh`

**Libraries tab**
- Click *Install New* → *Upload* → *Python Whl*
- Select `geobrix-${geobrix_version}-py3-none-any.whl`

Restart (or start) the cluster.

## Step 5: Verify

In a notebook attached to the cluster:

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

n = spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_*'").count()
print(f"✅ GeoBrix installed: {n} raster functions registered.")
```

If `n > 0`, installation succeeded. Don't list the full function set — it's noisy and the count is enough to confirm install. Users who want the catalog can find it in `references/functions.md` or via `SHOW FUNCTIONS LIKE 'gbx_rst_*'` directly.

## Automating with Databricks SDK (optional)

For repeatable bootstrap (e.g., across SA demos), use the SDK. **Read the warning below first** — this is the most likely place to silently break a customer's cluster config.

### ⚠️ Critical: `clusters.edit()` is full-replace, not merge

The legacy `w.clusters.edit(cluster_id=..., init_scripts=...)` call does NOT merge with the existing cluster spec. It is a **full PUT** — any field not explicitly passed is reset to default / blank. This is a well-known footgun that has wiped:

- `cluster_name` (rendered as the cluster's display name)
- `custom_tags` (used for cost tracking and governance)
- `spark_conf` (custom Spark configuration)
- `spark_env_vars`
- `policy_id` (cluster created from a policy gets detached)
- `single_user_name` (Dedicated mode user assignment)
- `cluster_log_conf`
- `ssh_public_keys`, `instance_pool_id`, cloud-specific attributes, etc.

**Never call `clusters.edit()` with a partial spec.** Cherry-picking fields is fragile because Databricks adds new cluster spec fields over time, and any missing field gets reset.

### Safe pattern — use `clusters.update()` (partial update)

`clusters.update()` uses a JSON-merge patch semantics: only the fields you specify change, everything else is left alone. **This is the preferred pattern.**

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    InitScriptInfo, VolumesStorageInfo, Library, ClusterAttributes
)

w = WorkspaceClient()
cluster_id = "${cluster_id}"  # substitute from parameters
volume_path = "${volume_path}"
geobrix_version = "${geobrix_version}"

# 1. Get current cluster to preserve existing init scripts (we APPEND, not REPLACE)
cluster = w.clusters.get(cluster_id)
existing_init_scripts = list(cluster.init_scripts or [])
new_init_dest = f"{volume_path}/geobrix-gdal-init.sh"
already_present = any(
    s.volumes and s.volumes.destination == new_init_dest
    for s in existing_init_scripts
)
if not already_present:
    existing_init_scripts.append(
        InitScriptInfo(volumes=VolumesStorageInfo(destination=new_init_dest))
    )

# 2. Partial update — only init_scripts changes; everything else preserved by Databricks
w.clusters.update(
    cluster_id=cluster_id,
    update_mask="init_scripts",
    cluster=ClusterAttributes(init_scripts=existing_init_scripts),
)

# 3. Install the WHL (libraries API is additive, safe)
w.libraries.install(
    cluster_id=cluster_id,
    libraries=[Library(whl=f"{volume_path}/geobrix-{geobrix_version}-py3-none-any.whl")],
)

# 4. Restart so init script runs
w.clusters.restart_and_wait(cluster_id=cluster_id)
```

### Fallback pattern — if `clusters.update()` isn't available

If on an SDK version that doesn't expose `update()`, use a **full re-serialize**: fetch the cluster, mutate the field, pass the entire spec back to `edit()`.

```python
import dataclasses
cluster = w.clusters.get(cluster_id)
new_spec = dataclasses.replace(cluster, init_scripts=existing_init_scripts)
# Pass the full spec back — every field is preserved because it's all there
w.clusters.edit(**new_spec.as_dict())
```

The risk with this pattern is if `as_dict()` drops fields or if the SDK's dataclass doesn't cover everything the API supports — but it's still vastly safer than cherry-picking.

### Zero-risk path: the UI

If you only need to configure one cluster (one-off SA setup), the UI path described in **Step 4a** has no full-replace risk. The Edit page only changes what you click; nothing else gets touched. Use this unless you specifically need automation.

### Hard rules for Claude when running cluster automation

1. **Never** call `w.clusters.edit(cluster_id=..., init_scripts=...)` with only the fields being changed.
2. **Always** prefer `w.clusters.update()` for cluster config changes.
3. **If `update()` is unavailable**, re-serialize the full cluster spec before calling `edit()`.
4. **After any automated cluster change**, tell the user to verify the cluster name, custom tags, and Spark conf are intact in the UI.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `SHOW FUNCTIONS LIKE 'gbx_rst_*'` is empty | Init script didn't run, or WHL not installed | Check cluster event log → Init script execution; reinstall WHL |
| `UnsatisfiedLinkError: libgdalalljni.so` | `.so` not in `/usr/lib/` | Confirm init script copied `.so`; check `VOL_DIR` value |
| `ClassNotFoundException: com.databricks.labs.gbx.*` | JAR not on classpath | Confirm init script copied JAR to `/databricks/jars/`; restart cluster |
| `ModuleNotFoundError: databricks.labs.gbx` | WHL not installed at cluster level | Re-add WHL via Libraries tab; restart |
| Init script fails on `apt update` | DNS / network issue from cluster | Check NSG/firewall to Ubuntu repos |
| Permission denied reading from Volume | UC permissions on Volume | Grant `READ VOLUME` to the cluster's service principal |

## Notes on serverless

GeoBrix does **not** run on Serverless compute. The GDAL native dependency requires cluster-level installation, which Serverless doesn't expose. If your workload requires Serverless, consider:

- Using native DBSQL `ST_` functions (public preview, DBR 17.1+) for vector ops
- Pre-processing rasters on a classic cluster and persisting to Delta, then querying from Serverless

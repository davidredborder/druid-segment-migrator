# Druid Segment Migrator (`druid-segment-migrator`)

A configuration-driven CLI tool (`rb_druid_migrator` / `rb_dsm`) to export Druid segments from a **Legacy** redBorder machine (`Druid 0.12.3`), convert the segment archive and metadata locally for **NG** (`Druid 31.0.1`), and import them into the destination NG machine.

---

## Architecture & Technical Background

### 1. Component Comparison

| Component | Legacy (`10.1.33.48`) | NG (`10.1.33.160`) | Notes / Migration Action |
|---|---|---|---|
| **Druid Version** | `0.12.3` | `31.0.1` | Formats are v9 compatible; schema & metadata differ |
| **Deep Storage** | Riak S3 (`s3cmd`) | MinIO (`mcli`) | Target bucket changed from `redborder` to `bucket` |
| **Deep Storage Base Key** | `s3://redborder/rbdata` | `<hostname>/bucket/rbdata` | MinIO paths include hostname prefix in `mcli` |
| **Metadata DB** | PostgreSQL (`druid` DB, table `druid_segments`) | PostgreSQL (`druid` DB, table `druid_segments`) | PostgreSQL column schema has added fields in NG |
| **DB CLI Helper** | `rb_psql druid` | `rb_psql druid` | Standard RedBorder CLI wrapper for `psql` |
| **Metadata Script** | `/opt/rb/bin/rb_druid_metadata.rb` | `/usr/lib/redborder/bin/rb_druid_metadata` (`rvm ruby-3.4.11@web`) | Invoked on NG to insert/update metadata from `rule.json` |
| **Backup / Import Tool** | `/opt/rb/bin/rb_backup_segments.sh` | `/usr/lib/redborder/bin/rb_export_import_segments.sh` | Shell scripts packaging & deploying segment archives |

### 2. End-to-End Migration Flow

```
+-------------------------------------------------------------------+
| Step 1: export (Legacy 10.1.33.48 - Druid 0.12.3)                 |
|    Run rb_backup_segments.sh -n -f /tmp/legacy-segments.tar.gz    |
|    Exports:                                                       |
|      - ./conf/db-druid-dump.psql                                  |
|      - ./segments/<ds>/<interval>/<ver>/<part>/index.zip          |
|      - ./segments/<ds>/<interval>/<ver>/<part>/descriptor.json    |
|      - ./segments/<ds>/<interval>/<ver>/<part>/rule.json          |
+-------------------------------------------------------------------+
                                 |
                                 | Step 2: get-tar (SCP Legacy -> Local)
                                 v
+-------------------------------------------------------------------+
| Step 3: convert (Local Workstation)                               |
|    Convert archive for NG (31.0.1) compatibility:                 |
|      - Keep index.zip as-is (Druid v9 format is read-compatible)   |
|      - Rewrite UUIDs in 00000.smoosh string dictionary if mapped  |
|      - Keep ./conf/db-druid-dump.psql (required by NG check)      |
|      - Remove descriptor.json (not used in Druid 31.0.1)          |
|      - Update each rule.json (or synthesize from descriptor.json):|
|          * Add "used_status_last_updated" (NOT NULL in NG DB)     |
|          * Set payload.loadSpec.bucket = "bucket"                 |
|          * Ensure payload.loadSpec.key matches relative path      |
|          * Apply optional datasource_map / uuid_map renames       |
|          * Bump version to current UTC if --bump-version enabled  |
|      - Repackage as gzip-compressed tar ending in .tar            |
+-------------------------------------------------------------------+
                                 |
                                 | Step 4: send-tar (SCP Local -> NG)
                                 v
+-------------------------------------------------------------------+
| Step 5: import (NG 10.1.33.160 - Druid 31.0.1)                    |
|    Run rb_export_import_segments.sh -r -e -n -v -f <file.tar>     |
|      - Uploads index.zip to MinIO (<hostname>/bucket/rbdata/...)  |
|      - Inserts/updates rule.json in PostgreSQL druid_segments     |
+-------------------------------------------------------------------+
                                 |
                                 | Step 6: verify (Read-only check on NG)
                                 v
+-------------------------------------------------------------------+
| Step 6: verify (NG 10.1.33.160)                                   |
|    Verify segment rows in PostgreSQL and index.zip in MinIO       |
+-------------------------------------------------------------------+
```

### 3. Format & Schema Differences

#### 3.1 Segment Binary (`index.zip`)
- Both Druid `0.12.3` and Druid `31.0.1` use **v9** segments (`version.bin` = `9`, `factory.json` = `{"type":"mMapSegmentFactory"}`, `00000.smoosh`, `meta.smoosh`).
- Legacy uses `"concise"` bitmaps and v1 column encodings inside `00000.smoosh`. Druid `31.0.1` maintains full read compatibility for v9 segments with concise bitmaps, so `index.zip` does not require conversion unless UUID remapping is configured.
- When `conversion.uuid_map` is configured, `migrate.py` inspects `meta.smoosh`, parses each `STRING` column's `GenericIndexed` dictionary inside `00000.smoosh`, and performs in-place byte substitution for mapped 36-character UUIDs across all string dimensions (`sensor_uuid`, `proxy_uuid`, `namespace_uuid`, `organization_uuid`, etc.).

#### 3.2 Deep Storage Layout (`Riak S3` vs `MinIO`)
- **Legacy S3 path**:
  `s3://redborder/rbdata/<datasource>/<interval>/<version>/<partition>/index.zip` (plus `descriptor.json`)
- **NG MinIO path**:
  `<hostname>/bucket/rbdata/<datasource>/<interval>/<version>/<partition>/index.zip`
- **`descriptor.json`**: Druid `31.0.1` stores segment descriptors exclusively in PostgreSQL (`druid_segments.payload`) and no longer stores `descriptor.json` in deep storage. During import, `rb_export_import_segments.sh` uploads only `*.zip` files to MinIO.

#### 3.3 PostgreSQL `druid_segments` Table & `rule.json`
Comparing `druid_segments` between Legacy (`0.12.3`) and NG (`31.0.1`):

| Column / JSON Key | Legacy (`0.12.3`) | NG (`31.0.1`) | Conversion Action |
|---|---|---|---|
| `id` | `<ds>_<start>_<end>_<version>[_<part>]` | Same | Preserved (recomputed if datasource or version changes) |
| `datasource` | `<ds>` | Same | Preserved (or renamed via `datasource_map` / `uuid_map`) |
| `created_date` | ISO-8601 string | ISO-8601 string | Preserved |
| `start` / `end` | ISO-8601 strings | ISO-8601 strings | Preserved |
| `partitioned` | `"t"` / `"f"` | `"t"` / `"f"` | Preserved |
| `version` | ISO-8601 string | ISO-8601 string | Preserved (or updated to current UTC if `--bump-version`) |
| `used` | `"t"` / `"f"` | `"t"` / `"f"` | Set to `"t"` when `enable_segments: true` |
| `used_status_last_updated` | *(Missing in Legacy DB)* | `varchar(255) NOT NULL` | **Added to `rule.json`**: set to segment `created_date` / current UTC timestamp |
| `upgraded_from_segment_id` | *(Missing in Legacy DB)* | `varchar(255) NULL` | Handled as `NULL` by NG metadata script |
| `payload.loadSpec.bucket` | `"redborder"` | `"bucket"` | **Changed to `"bucket"`** (`conversion.target_bucket`) |
| `payload.loadSpec.key` | `"rbdata/<ds>/<interval>/<ver>/<part>/index.zip"` | `"rbdata/<ds>/<interval>/<ver>/<part>/index.zip"` | Ensured to match `<target_base_key>/<ds>/<interval>/<ver>/<part>/index.zip` |

#### 3.4 Upstream Script Validation & Quirks
- **NG Archive Validation**: `rb_export_import_segments.sh` requires:
  1. Archive must be gzip-compressed (`.tar` filename, but gzip payload).
  2. Archive must contain `./conf/db-druid-dump.psql` (used by the script's sanity checks even without full DB restore).
  3. Archive must have at least 4 directory levels under `./segments/` (`./segments/<ds>/<interval>/<ver>/<part>/`).
  4. Each segment folder must contain `rule.json` and `index.zip`.
- **Known Script Quirks**:
  - `legacy/rb_backup_segments.sh` line 521 checks `if [ -s $f ]` with an unset variable `$f`, causing the console output to print `FAILED` when creating `rule.json` even though the file is successfully created. `migrate.py` validates all `rule.json` files and can synthesize missing/empty ones from `descriptor.json`.
  - `ng/rb_export_import_segments.sh` has a known bug in its `-x`/`-y` date filtering flag (`awk -F '/' '{print $3}'` extracts temporary directory paths rather than timestamps). `migrate.py` solves this by filtering segments locally with Python during the `convert` step.

---

## Prerequisites

On your local machine:
- `python3` (3.9+)
- `openssh-clients` (`ssh`, `scp`)
- `sshpass` (if using password authentication in `config.yaml`)

---

## Installation

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd druid-segment-migrator
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure CLI symlinks are executable (created by default):
   ```bash
   chmod +x rb_druid_migrator rb_dsm migrate.py
   ```

---

## Configuration (`config.yaml`)

Edit [config.yaml](./config.yaml) before running the tool:

```yaml
legacy:
  host: "10.1.33.48"
  port: 22
  user: "root"
  password: "password"
  ssh_extra_args:
    - "-o"
    - "StrictHostKeyChecking=no"
    - "-o"
    - "HostKeyAlgorithms=+ssh-rsa"
    - "-o"
    - "PubkeyAcceptedKeyTypes=+ssh-rsa"
  backup_script: "/opt/rb/bin/rb_backup_segments.sh"
  remote_tar_path: "/tmp/legacy-segments.tar.gz"
  filter: ""                  # Optional grep filter (-g), e.g. "rb_flow" or "2026-10-22"

local:
  work_dir: "./work"
  raw_tar_path: "./work/legacy-segments.tar.gz"
  converted_tar_path: "./work/ng-segments.tar"   # Must end with .tar

conversion:
  target_bucket: "bucket"     # MinIO bucket name on NG (replaces "redborder")
  target_base_key: "rbdata"   # Base key prefix in MinIO
  enable_segments: true       # Sets "used": "t" (or "f" if false) in rule.json
  bump_version: false         # Sets segment version to current UTC timestamp so imported segments supersede existing NG segments
  datasource_map: {}          # Optional rename map, e.g. {"rb_flow_old_uuid": "rb_flow_new_uuid"}
  uuid_map: {}                # Optional 36-char UUID map (rewrites index.zip smoosh + rb_flow_<uuid> datasource names)
  datasource_regex: ""        # Optional regex filter on datasource name, e.g. "^(rb_flow_.*|rb_event)$"
  start_date: ""              # Optional ISO-8601 start filter, e.g. "2026-10-22T18:00:00.000Z"
  stop_date: ""               # Optional ISO-8601 stop filter,  e.g. "2026-10-22T20:00:00.000Z"

ng:
  host: "10.1.33.160"
  port: 22
  user: "root"
  password: "password"
  ssh_extra_args:
    - "-o"
    - "StrictHostKeyChecking=no"
  import_script: "/usr/lib/redborder/bin/rb_export_import_segments.sh"
  remote_tar_path: "/var/backup/segments/ng-segments.tar"
  enable_on_import: true      # Passes -e to rb_export_import_segments.sh
  restore_full_db: false      # Keep false so existing NG segments in PostgreSQL are NOT deleted
```

---

## Usage

The CLI can be invoked using `./rb_druid_migrator` (or the short alias `./rb_dsm`, or `./migrate.py`):

```bash
./rb_druid_migrator [-c CONFIG] [--dry-run] [--filter FILTER] [--datasource-regex REGEX] [--start-date ISO] [--stop-date ISO] [--run-dir RUN_DIR] [--bump-version] {export,get-tar,convert,send-tar,import,verify,clean,rollback,all}
```

### 1. Preview Commands (`--dry-run`)
Always preview what will run without touching any machine or file:
```bash
./rb_druid_migrator -c config.yaml --dry-run all
```

### 2. Run Step-by-Step (Recommended)

#### Step 1 — `export`: Create backup tar on Legacy (`10.1.33.48`)
Runs `/opt/rb/bin/rb_backup_segments.sh` on the Legacy machine to export segments from Riak S3 and metadata from PostgreSQL into `legacy.remote_tar_path`:
```bash
./rb_druid_migrator -c config.yaml export
```

#### Step 2 — `get-tar`: Download backup tar to Local Machine
Creates a new non-overwriting timestamped folder `./work/<YYYYMMDD_HHMMSS>/` (or `./work/<run-dir>/` if `--run-dir` is given) and downloads `legacy.remote_tar_path` into `./work/<run_id>/legacy-segments.tar.gz` (refuses to overwrite if the file already exists):
```bash
./rb_druid_migrator -c config.yaml get-tar
```

#### Step 3 — `convert`: Convert archive locally for NG (`31.0.1`)
Converts any pending `./work/<run_id>/legacy-segments.tar.gz` directory that does not yet have `ng-segments.tar` (never overwriting existing converted tars). Updates each segment's `rule.json` (adds `used_status_last_updated`, changes `payload.loadSpec.bucket` to `"bucket"`, applies any `datasource_map`, `uuid_map` (rewriting both `rb_flow_<uuid>` datasource names and `00000.smoosh` string dictionary UUIDs inside `index.zip`), `datasource_regex`, or `start_date`/`stop_date` filters, and removes `descriptor.json`), and packages the result into `./work/<run_id>/ng-segments.tar`:
```bash
./rb_druid_migrator -c config.yaml convert
```

#### Step 4 — `send-tar`: Upload converted tar(s) to NG (`10.1.33.160`)
Uploads all converted `./work/<run_id>/ng-segments.tar` batches in chronological order (or a specific `--run-dir`) to `/var/backup/segments/<run_id>/ng-segments.tar` on NG:
```bash
./rb_druid_migrator -c config.yaml send-tar
```

#### Step 5 — `import`: Import segments on NG (`10.1.33.160`)
Runs `/usr/lib/redborder/bin/rb_export_import_segments.sh -r -n -v -e -f /var/backup/segments/<run_id>/ng-segments.tar` on NG for each converted batch to upload `index.zip` files to MinIO and insert/update metadata rows in PostgreSQL `druid_segments`:
```bash
./rb_druid_migrator -c config.yaml import
```

#### Step 6 — `verify`: Verify imported segments on NG (`10.1.33.160`)
Runs read-only queries against PostgreSQL (`rb_psql druid`) and MinIO (`mcli find`) on NG to confirm the segments are present and enabled:
```bash
./rb_druid_migrator -c config.yaml verify
```

#### Step 7 — `clean`: Clean local `./work/` directories
Removes all run directories inside `./work/` (or only `./work/<run-dir>` if `--run-dir` is passed):
```bash
./rb_druid_migrator -c config.yaml clean
```

#### Step 8 — `rollback`: Wipe imported segments on NG (`10.1.33.160`)
Removes matching segments on NG across PostgreSQL (`druid_segments`), MinIO (`s3://bucket/rbdata/`), and Druid Historical local cache (`/var/druid/historical/indexCache`), and re-enables (`used = true`) any older NG segments in the affected intervals that were overshadowed by `--bump-version`:
```bash
# Rollback a specific batch folder:
./rb_druid_migrator -c config.yaml --run-dir 20261023_100000 rollback

# Or rollback segments on NG matching a datasource regex and date window:
./rb_druid_migrator -c config.yaml --datasource-regex '^rb_monitor' --start-date "2026-10-23T00:00:00Z" --stop-date "2026-10-23T09:00:00Z" rollback
```

---

### 3. Run All Steps at Once
To run `export` -> `get-tar` -> `convert` -> `send-tar` -> `import` -> `verify` in one command:
```bash
./rb_druid_migrator -c config.yaml all
```

---

## Common Examples

- **Export only a single datasource, multiple datasources, or a specific day from Legacy (`--filter`)**:
  ```bash
  # Single datasource:
  ./migrate.py -c config.yaml --filter "rb_monitor" export
  # All rb_flow_* datasources for a specific day (e.g. 2026-10-23):
  ./migrate.py -c config.yaml --filter "rb_flow_.*/2026-10-23" export
  # Multiple datasources via BRE grep alternation:
  ./migrate.py -c config.yaml --filter 'rb_flow\|rb_event' export
  ```
- **Convert & import one or multiple datasources from an already-downloaded tar (`--datasource-regex`)**:
  ```bash
  # Exact single datasource:
  ./migrate.py -c config.yaml --datasource-regex '^rb_event$' convert
  # Multiple datasources via Python regex:
  ./migrate.py -c config.yaml --datasource-regex '^(rb_flow_.*|rb_event)$' convert
  ```
- **Convert & import a specific date range (`--start-date` / `--stop-date`)**:
  ```bash
  ./migrate.py -c config.yaml \
    --start-date "2026-10-22T18:00:00.000Z" \
    --stop-date "2026-10-22T20:00:00.000Z" \
    convert
  ```
- **Remap sensor/organization/namespace UUIDs when sensors were recreated manually on NG (`conversion.uuid_map`)**:
  In `config.yaml`, map each Legacy 36-char UUID to its new NG UUID:
  ```yaml
  conversion:
    uuid_map:
      "78b73529-63a4-4b1f-9d45-3e48f0eb0db0": "7c3c62f1-728f-41e5-aa69-e44bcd8830fa" # namespace_uuid (also renames rb_flow_<uuid>)
      "1199e7b3-1f84-4ade-bd26-44026b11291d": "2c07ead9-6198-4f36-bb67-6beeff35c5c7" # organization_uuid
      "eaaffc57-804a-4c30-b86a-39d3ce9597a9": "377a3c45-a530-4896-af58-16c0e7887974" # service_provider_uuid
      "5dad542b-870b-4aeb-bc91-fbefe43af191": "d3510093-1cd1-4fa2-bf00-1c6c5f3e2f59" # sensor_uuid
  ```
- **Bump segment `version` so imported segments supersede existing/compacted NG segments (`--bump-version`)**:
  ```bash
  ./migrate.py -c config.yaml --bump-version convert
  ./migrate.py -c config.yaml send-tar
  ./migrate.py -c config.yaml import
  ```
- **Clean `./work/` (or a specific batch folder) and re-run conversion**:
  ```bash
  # Clean only one batch folder:
  ./migrate.py -c config.yaml --run-dir 20261023_100000 clean
  # Or clean all batches inside ./work/:
  ./migrate.py -c config.yaml clean
  ```
- **Rollback / wipe imported segments from NG (`rollback`)**:
  Wipes matching segments from NG PostgreSQL (`druid_segments`), MinIO (`s3://bucket/rbdata/`), and Druid Historical cache (`/var/druid/historical/indexCache`), and automatically re-enables (`used = true`) any older NG segments in the same interval that were overshadowed by `--bump-version`:
  ```bash
  # Mode A: Rollback the exact segments imported from a specific batch folder:
  ./migrate.py -c config.yaml --run-dir 20261023_100000 rollback

  # Mode B: Wipe segments on NG matching a datasource regex and/or date range:
  ./migrate.py -c config.yaml \
    --datasource-regex '^rb_monitor_7c3c62f1-728f-41e5-aa69-e44bcd8830fa$' \
    --start-date "2026-10-23T00:00:00.000Z" \
    --stop-date "2026-10-23T09:00:00.000Z" \
    rollback
  ```

---

## Understanding `--bump-version` (`conversion.bump_version`)

### Goal
In Apache Druid, when two segments in the same datasource overlap in time (`[start, end)`), Druid only serves the segment with the **higher `version` string** (`used = 't'`) and automatically marks the lower-version segment as overshadowed (`used = 'f'`).

- **Legacy (Druid 0.12.3):** Always assigns segment versions on the exact hour (`.000Z`, e.g. `2026-10-23T07:00:00.000Z`).
- **NG (Druid 31.0.1):** When NG is already running, its Kafka supervisor creates hourly segments with versions a few hundred milliseconds after the hour (e.g. `2026-10-23T07:00:00.384Z`), and its nightly compaction task creates 24-hour segments with a next-day version (e.g. `2026-10-23T01:08:10.096Z`).

Without `--bump-version`, if you import a Legacy segment (`version = ...07:00:00.000Z`) into an NG cluster that already has a segment for that same datasource and time range (`version = ...07:00:00.384Z`), Druid Coordinator immediately marks the imported Legacy segment `used = 'f'` because `.000Z < .384Z`.

Passing `--bump-version` (or setting `conversion.bump_version: true` in `config.yaml`) updates the converted segment's `version` to the **current UTC timestamp** at conversion time so it has a newer version than any pre-existing NG segment.

### Consequences
1. **Overlapping hourly segments on NG are replaced by the imported Legacy segment:**
   For any `[start, end)` hour where NG already had its own segment in the same datasource, the imported Legacy segment (with its newer `version`) stays `used = 't'` and Druid Coordinator marks the older NG segment for that hour `used = 'f'`.
2. **Overlapping 24-hour compacted segments on NG are partially superseded for the imported hours:**
   If NG already compacted a full day (`00:00–24:00`) and you import a 1-hour Legacy segment (`14:00–15:00`) with `--bump-version`, Druid keeps **both** segments `used = 't'`: it serves your imported Legacy segment for `14:00–15:00` and serves the 24-hour NG segment for the remaining hours of the day.
3. **No effect when the datasource or time range does not exist on NG yet:**
   `version` only breaks ties between overlapping segments — it never modifies the segment's actual time interval (`start`, `end`) or the `__time` data inside `index.zip`. On a fresh NG installation with no existing segments for those hours, `--bump-version` and normal conversion behave identically.
4. **Coordinator load delay (~15–60 seconds):**
   After `./migrate.py import` updates PostgreSQL `druid_segments`, Druid Coordinator polls metadata and assigns the new `used = 't'` segments to Historical nodes on its next cycle (typically 15–60 seconds before data appears in the Web UI and Druid SQL).

---

## Running Unit Tests

```bash
python3 -m unittest -v test_migrate.py
```

---

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).


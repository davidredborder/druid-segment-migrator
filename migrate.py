#!/usr/bin/env python3
"""
Step-by-step Druid segment migration tool (Legacy 0.12.3 -> NG 31.0.1).

Steps:
  1. export   : Export segments and metadata tar on Legacy machine
  2. get-tar  : Download legacy tar archive to local machine
  3. convert  : Convert archive structure and rule.json metadata for NG (31.0.1)
  4. send-tar : Upload converted tar archive to NG machine
  5. import   : Import segments into MinIO and PostgreSQL on NG machine
  6. verify   : Run read-only verification against PostgreSQL and MinIO on NG
  7. all      : Run all steps sequentially
"""

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import warnings
import zipfile
import yaml


UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_iso_utc(val, field_name: str, is_end: bool = False):
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    if isinstance(val, date):
        if is_end:
            return datetime(val.year, val.month, val.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
        return datetime(val.year, val.month, val.day, 0, 0, 0, tzinfo=timezone.utc)

    s = str(val).strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            d = datetime.strptime(s, "%Y-%m-%d").date()
            if is_end:
                return datetime(d.year, d.month, d.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
            return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        norm = s[:-1] + "+00:00" if s.endswith(("Z", "z")) else s
        dt = datetime.fromisoformat(norm)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 timestamp for {field_name}: {val!r}") from exc


def safe_extractall(tf: tarfile.TarFile, dest: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        if hasattr(tarfile, "data_filter"):
            tf.extractall(dest, filter="data")
        else:
            tf.extractall(dest)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ssh_prefix(host_cfg: dict) -> list:
    cmd = []
    password = host_cfg.get("password")
    if password:
        cmd.extend(["sshpass", "-p", str(password)])
    cmd.append("ssh")
    port = host_cfg.get("port", 22)
    if port:
        cmd.extend(["-p", str(port)])
    cmd.extend(host_cfg.get("ssh_extra_args", []))
    cmd.append(f"{host_cfg['user']}@{host_cfg['host']}")
    return cmd


def _scp_prefix(host_cfg: dict) -> list:
    cmd = []
    password = host_cfg.get("password")
    if password:
        cmd.extend(["sshpass", "-p", str(password)])
    cmd.append("scp")
    port = host_cfg.get("port", 22)
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend(host_cfg.get("ssh_extra_args", []))
    return cmd


def run_cmd(cmd: list, dry_run: bool = False, input_text: str = None) -> int:
    printable = " ".join(shlex.quote(str(x)) for x in cmd)
    if dry_run:
        print(f"[DRY-RUN] {printable}")
        return 0
    print(f"[EXEC] {printable}")
    env = os.environ.copy()
    env.setdefault("OPENSSL_ENABLE_SHA1_SIGNATURES", "1")
    kwargs = {"check": False, "env": env}
    if input_text is not None:
        kwargs["input"] = input_text
        kwargs["text"] = True
    res = subprocess.run(cmd, **kwargs)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {res.returncode}: {printable}")
    return res.returncode


def build_export_cmd(cfg: dict) -> list:
    leg = cfg["legacy"]
    remote_cmd = f"{leg['backup_script']} -n -f {shlex.quote(leg['remote_tar_path'])}"
    if leg.get("filter"):
        remote_cmd += f" -g {shlex.quote(str(leg['filter']))}"
    return _ssh_prefix(leg) + [f"bash -lc {shlex.quote(remote_cmd)}"]


def _raw_tar_filename(loc: dict) -> str:
    return Path(loc.get("raw_tar_path", "legacy-segments.tar.gz")).name


def _converted_tar_filename(loc: dict) -> str:
    return Path(loc.get("converted_tar_path", "ng-segments.tar")).name


def _validate_run_id(run_id: str) -> str:
    rid = str(run_id).strip()
    if not rid or rid in (".", "..") or not RUN_ID_RE.match(rid):
        raise ValueError(
            f"Invalid run_dir name {run_id!r}: must be a single directory name containing only [A-Za-z0-9._-]"
        )
    return rid


def _generate_run_id(work_dir: Path) -> str:
    base = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    candidate = base
    idx = 1
    while (work_dir / candidate).exists():
        candidate = f"{base}_{idx}"
        idx += 1
    return candidate


def build_get_tar_cmd(cfg: dict, target_raw: Path = None) -> list:
    leg = cfg["legacy"]
    loc = cfg["local"]
    dest = str(target_raw) if target_raw is not None else str(loc["raw_tar_path"])
    return _scp_prefix(leg) + [
        f"{leg['user']}@{leg['host']}:{leg['remote_tar_path']}",
        dest,
    ]


def build_send_tar_cmd(cfg: dict, local_tar: Path = None, remote_tar: str = None) -> list:
    ng = cfg["ng"]
    loc = cfg["local"]
    src = str(local_tar) if local_tar is not None else str(loc["converted_tar_path"])
    dst_remote = str(remote_tar) if remote_tar is not None else str(ng["remote_tar_path"])
    return _scp_prefix(ng) + [
        src,
        f"{ng['user']}@{ng['host']}:{dst_remote}",
    ]


def build_import_cmd(cfg: dict, remote_tar: str = None) -> list:
    ng = cfg["ng"]
    dst_remote = str(remote_tar) if remote_tar is not None else str(ng["remote_tar_path"])
    flags = ["-r", "-n", "-v"]
    if ng.get("enable_on_import", True):
        flags.append("-e")
    if ng.get("restore_full_db", False):
        flags.append("-b")
    flags.extend(["-f", shlex.quote(dst_remote)])
    remote_cmd = f"{ng['import_script']} {' '.join(flags)}"
    return _ssh_prefix(ng) + [f"bash -lc {shlex.quote(remote_cmd)}"]


def build_verify_cmd(cfg: dict) -> list:
    ng = cfg["ng"]
    base_key = cfg.get("conversion", {}).get("target_base_key", "rbdata")
    bucket = cfg.get("conversion", {}).get("target_bucket", "bucket")
    remote_cmd = (
        'echo "=== NG PostgreSQL druid_segments summary ==="; '
        'echo "SELECT datasource, used, count(*) FROM druid_segments GROUP BY datasource, used ORDER BY datasource;" | rb_psql druid; '
        'echo "=== NG MinIO segment count ==="; '
        f'/usr/local/bin/mcli find "$(hostname -s)/{bucket}/{base_key}/" --name "index.zip" | wc -l'
    )
    return _ssh_prefix(ng) + [f"bash -lc {shlex.quote(remote_cmd)}"]


def _synthesize_rule_from_descriptor(desc: dict, ds: str, interval_dir: str, version: str, part: str) -> dict:
    start_str, end_str = interval_dir.split("_", 1)
    part_num = int(part)
    part_suffix = f"_{part_num}" if part_num != 0 else ""
    seg_id = f"{ds}_{start_str}_{end_str}_{version}{part_suffix}"
    return {
        "id": seg_id,
        "datasource": ds,
        "created_date": version,
        "start": start_str,
        "end": end_str,
        "partitioned": "t",
        "version": version,
        "used": "t",
        "payload": dict(desc),
    }


def _validate_uuid_map(raw_map) -> dict:
    if not raw_map:
        return {}
    if not isinstance(raw_map, dict):
        raise ValueError("conversion.uuid_map must be a dictionary of {old_uuid: new_uuid}")
    validated = {}
    for k, v in raw_map.items():
        ks = str(k).strip().lower()
        vs = str(v).strip().lower()
        if not UUID_RE.match(ks) or not UUID_RE.match(vs):
            raise ValueError(
                f"Invalid 36-char UUID in conversion.uuid_map: {k!r} -> {v!r}"
            )
        validated[ks] = vs
    return validated


def _resolve_target_datasource(ds: str, conv_cfg: dict, uuid_map: dict = None) -> str:
    ds_map = conv_cfg.get("datasource_map") or {}
    if ds in ds_map:
        return ds_map[ds]
    umap = uuid_map if uuid_map is not None else _validate_uuid_map(conv_cfg.get("uuid_map"))
    if not umap:
        return ds
    pattern = re.compile("|".join(re.escape(k) for k in umap), re.IGNORECASE)
    return pattern.sub(lambda m: umap[m.group(0).lower()], ds)


def rewrite_index_zip_uuids(src_zip: Path, dst_zip: Path, uuid_map: dict) -> None:
    if not uuid_map:
        shutil.copy2(src_zip, dst_zip)
        return

    byte_map = {k.lower().encode("ascii"): v.lower().encode("ascii") for k, v in uuid_map.items()}
    byte_pattern = re.compile(b"|".join(re.escape(k) for k in byte_map), re.IGNORECASE)

    with zipfile.ZipFile(src_zip, "r") as zin:
        meta_ranges = []
        if "meta.smoosh" in zin.namelist():
            meta_lines = zin.read("meta.smoosh").decode("utf-8", errors="replace").splitlines()
            for line in meta_lines[1:]:
                parts = line.split(",")
                if len(parts) == 4 and parts[0] not in ("__time", "index.drd", "metadata.drd"):
                    try:
                        meta_ranges.append((int(parts[1]), int(parts[2]), int(parts[3])))
                    except ValueError:
                        pass

        with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename.endswith(".smoosh") and item.filename != "meta.smoosh":
                    smoosh_idx = None
                    try:
                        smoosh_idx = int(Path(item.filename).stem)
                    except ValueError:
                        pass
                    if meta_ranges and smoosh_idx is not None:
                        buf = bytearray(data)
                        for f_idx, start, end in meta_ranges:
                            if f_idx == smoosh_idx and 0 <= start < end <= len(buf) and (end - start) >= 19:
                                desc_len = struct.unpack(">I", buf[start : start + 4])[0]
                                desc_end = start + 4 + desc_len
                                if desc_end + 15 <= end and b'"valueType":"STRING"' in buf[start + 4 : desc_end]:
                                    gi_start = desc_end + 5
                                    _, _, gi_bytes, _ = struct.unpack(">BBII", buf[gi_start : gi_start + 10])
                                    dict_start = gi_start + 10
                                    dict_end = gi_start + 6 + gi_bytes
                                    if dict_start < dict_end <= end:
                                        buf[dict_start:dict_end] = byte_pattern.sub(
                                            lambda m: byte_map[m.group(0).lower()], bytes(buf[dict_start:dict_end])
                                        )
                                    else:
                                        buf[desc_end:end] = byte_pattern.sub(
                                            lambda m: byte_map[m.group(0).lower()], bytes(buf[desc_end:end])
                                        )
                        data = bytes(buf)
                    else:
                        data = byte_pattern.sub(lambda m: byte_map[m.group(0).lower()], data)
                zout.writestr(item, data)


def transform_segment_metadata(
    rule_data: dict,
    ds: str,
    interval_dir: str,
    version: str,
    part: str,
    conv_cfg: dict,
) -> tuple:
    target_ds = _resolve_target_datasource(ds, conv_cfg)
    target_bucket = conv_cfg.get("target_bucket", "bucket")
    target_base_key = conv_cfg.get("target_base_key", "rbdata")
    enable_segments = conv_cfg.get("enable_segments", True)
    effective_version = (
        str(conv_cfg.get("_bumped_version") or utc_now_iso())
        if conv_cfg.get("bump_version", False)
        else version
    )

    start_str, end_str = interval_dir.split("_", 1)
    part_num = int(part)
    part_suffix = f"_{part_num}" if part_num != 0 else ""
    seg_id = f"{target_ds}_{start_str}_{end_str}_{effective_version}{part_suffix}"

    rule = dict(rule_data)
    payload = dict(rule.get("payload") or {})
    load_spec = dict(payload.get("loadSpec") or {"type": "s3_zip", "S3Schema": "s3n"})
    shard_spec = dict(payload.get("shardSpec") or {"type": "linear", "partitionNum": part_num})
    shard_spec["partitionNum"] = part_num

    load_spec["bucket"] = target_bucket
    load_spec["key"] = f"{target_base_key}/{target_ds}/{interval_dir}/{effective_version}/{part}/index.zip"

    payload["dataSource"] = target_ds
    payload["version"] = effective_version
    payload["identifier"] = seg_id
    payload["loadSpec"] = load_spec
    payload["shardSpec"] = shard_spec

    rule["id"] = seg_id
    rule["datasource"] = target_ds
    rule["created_date"] = rule.get("created_date") or effective_version
    rule["start"] = rule.get("start") or start_str
    rule["end"] = rule.get("end") or end_str
    rule["partitioned"] = rule.get("partitioned") or "t"
    rule["version"] = effective_version
    rule["used"] = "t" if enable_segments else "f"
    if conv_cfg.get("bump_version", False):
        rule["used_status_last_updated"] = effective_version
    elif not rule.get("used_status_last_updated"):
        rule["used_status_last_updated"] = rule["created_date"] or utc_now_iso()
    rule["payload"] = payload

    return target_ds, rule


def _parse_segment_rel_parts(rel_parts: tuple) -> tuple:
    """
    Support both standard 5-part paths (<ds>/<interval>/<ver>/<part>/index.zip)
    and 6-part reindexed paths (<ds>/<ds>/<interval>/<ver>/<part>/index.zip).
    """
    if len(rel_parts) == 5:
        ds, interval_dir, version, part, _ = rel_parts
        return ds, interval_dir, version, part
    if len(rel_parts) == 6 and rel_parts[0] == rel_parts[1]:
        ds, _, interval_dir, version, part, _ = rel_parts
        return ds, interval_dir, version, part
    raise RuntimeError(
        f"Unexpected segment path depth under segments/: {'/'.join(rel_parts)}"
    )


def step_export(cfg: dict, dry_run: bool = False) -> None:
    run_cmd(build_export_cmd(cfg), dry_run=dry_run)


def step_get_tar(cfg: dict, dry_run: bool = False) -> None:
    loc = cfg.setdefault("local", {})
    work_dir = Path(loc.get("work_dir", "./work"))
    raw_run_id = loc.get("run_dir")
    run_id = _validate_run_id(raw_run_id) if raw_run_id is not None and str(raw_run_id).strip() != "" else (
        _validate_run_id(raw_run_id) if raw_run_id is not None else _generate_run_id(work_dir)
    )
    target_raw = work_dir / run_id / _raw_tar_filename(loc)
    if target_raw.exists():
        raise FileExistsError(
            f"Refusing to overwrite existing archive: {target_raw} (run 'clean' or use a new --run-dir)"
        )
    loc["_active_run_dir"] = run_id
    if not dry_run:
        target_raw.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(build_get_tar_cmd(cfg, target_raw=target_raw), dry_run=dry_run)


def resolve_convert_targets(cfg: dict, dry_run: bool = False) -> list:
    loc = cfg.get("local") or {}
    work_dir = Path(loc.get("work_dir", "./work"))
    raw_name = _raw_tar_filename(loc)
    out_name = _converted_tar_filename(loc)
    raw_run_id = loc.get("run_dir") if loc.get("run_dir") is not None else loc.get("_active_run_dir")
    run_id = _validate_run_id(raw_run_id) if raw_run_id is not None else ""

    if run_id:
        rdir = work_dir / run_id
        raw_p = rdir / raw_name
        out_p = rdir / out_name
        if not dry_run and out_p.exists():
            raise FileExistsError(
                f"Refusing to overwrite existing converted archive: {out_p}"
            )
        return [(raw_p, out_p)]

    if work_dir.is_dir():
        run_dirs_with_raw = [
            d for d in sorted(work_dir.iterdir()) if d.is_dir() and (d / raw_name).is_file()
        ]
        if run_dirs_with_raw:
            pending = [
                (d / raw_name, d / out_name)
                for d in run_dirs_with_raw
                if not (d / out_name).exists()
            ]
            if not pending and not dry_run:
                raise FileExistsError(
                    f"All run directories in {work_dir} already have {out_name}; refusing to overwrite"
                )
            return pending or [(run_dirs_with_raw[-1] / raw_name, run_dirs_with_raw[-1] / out_name)]

    raw_p = Path(loc["raw_tar_path"])
    out_p = Path(loc["converted_tar_path"])
    if not dry_run and out_p.exists():
        raise FileExistsError(
            f"Refusing to overwrite existing converted archive: {out_p}"
        )
    return [(raw_p, out_p)]


def _convert_single_archive(
    raw_tar: Path,
    out_tar: Path,
    conv_cfg: dict,
    uuid_map: dict,
    ds_pattern,
    start_dt,
    stop_dt,
    dry_run: bool = False,
    work_dir: Path = None,
) -> int:
    if dry_run:
        print(f"[DRY-RUN] Convert {raw_tar} -> {out_tar} (target_bucket={conv_cfg.get('target_bucket', 'bucket')})")
        return 1

    if not raw_tar.is_file():
        raise FileNotFoundError(f"Raw archive not found: {raw_tar}")
    if out_tar.exists():
        raise FileExistsError(f"Refusing to overwrite existing converted archive: {out_tar}")

    out_tar.parent.mkdir(parents=True, exist_ok=True)
    if out_tar.suffix != ".tar":
        raise ValueError(
            f"converted_tar_path must end with .tar for rb_export_import_segments.sh compatibility: {out_tar}"
        )

    with tempfile.TemporaryDirectory(prefix="druid_conv_") as tmp:
        tmp_in = Path(tmp) / "in"
        tmp_out = Path(tmp) / "out"
        tmp_in.mkdir()
        tmp_out.mkdir()

        with tarfile.open(raw_tar, "r:*") as tf:
            safe_extractall(tf, tmp_in)

        # 1. Preserve or create conf/db-druid-dump.psql (required by rb_export_import_segments.sh line 189)
        (tmp_out / "conf").mkdir(parents=True, exist_ok=True)
        in_dump = tmp_in / "conf" / "db-druid-dump.psql"
        out_dump = tmp_out / "conf" / "db-druid-dump.psql"
        if in_dump.is_file():
            shutil.copy2(in_dump, out_dump)
        else:
            out_dump.write_bytes(b"PLACEHOLDER_FOR_NG_IMPORT_CHECK\n")

        # 2. Walk segments/
        in_segments = tmp_in / "segments"
        if not in_segments.is_dir():
            raise RuntimeError(f"Missing 'segments/' directory in {raw_tar}")

        all_zips = sorted(in_segments.rglob("index.zip"))
        if not all_zips:
            print(
                f"[WARN] No segments found inside {raw_tar} "
                f"(segments may not be indexed on Legacy yet). Skipping conversion."
            )
            parent_dir = raw_tar.parent
            if (
                work_dir is not None
                and RUN_ID_RE.match(parent_dir.name)
                and parent_dir.parent.resolve() == work_dir.resolve()
                and list(parent_dir.iterdir()) == [raw_tar]
            ):
                raw_tar.unlink()
                parent_dir.rmdir()
            return 0

        converted_count = 0
        for zip_path in all_zips:
            rel_parts = zip_path.relative_to(in_segments).parts
            ds, interval_dir, version, part = _parse_segment_rel_parts(rel_parts)
            seg_start, seg_end = interval_dir.split("_", 1)
            mapped_ds = _resolve_target_datasource(ds, conv_cfg, uuid_map=uuid_map)

            if ds_pattern and not (ds_pattern.search(ds) or ds_pattern.search(mapped_ds)):
                continue
            if start_dt and _parse_iso_utc(seg_start, "segment.start") < start_dt:
                continue
            if stop_dt and _parse_iso_utc(seg_end, "segment.end") > stop_dt:
                continue

            seg_in_dir = zip_path.parent
            rule_path = seg_in_dir / "rule.json"
            desc_path = seg_in_dir / "descriptor.json"

            rule_data = None
            if rule_path.is_file() and rule_path.stat().st_size > 0:
                content = rule_path.read_text(encoding="utf-8").strip()
                if content:
                    rule_data = json.loads(content)

            if rule_data is None:
                if not desc_path.is_file():
                    raise RuntimeError(
                        f"Neither valid rule.json nor descriptor.json found in {seg_in_dir}"
                    )
                desc_data = json.loads(desc_path.read_text(encoding="utf-8"))
                rule_data = _synthesize_rule_from_descriptor(
                    desc_data, ds, interval_dir, version, part
                )

            target_ds, converted_rule = transform_segment_metadata(
                rule_data, ds, interval_dir, version, part, conv_cfg
            )
            effective_ver = converted_rule["version"]

            seg_out_dir = tmp_out / "segments" / target_ds / interval_dir / effective_ver / part
            seg_out_dir.mkdir(parents=True, exist_ok=True)
            rewrite_index_zip_uuids(zip_path, seg_out_dir / "index.zip", uuid_map)
            (seg_out_dir / "rule.json").write_text(
                json.dumps(converted_rule, indent=2) + "\n", encoding="utf-8"
            )
            converted_count += 1

        if converted_count == 0:
            print(
                f"[WARN] No matching segments found inside {raw_tar} "
                f"after applying conversion filters. Skipping conversion."
            )
            return 0

        # 3. Package as gzip-compressed tar archive atomically
        tmp_tar = out_tar.with_name(out_tar.name + ".tmp")
        try:
            with tarfile.open(tmp_tar, "w:gz") as tf:
                tf.add(tmp_out, arcname=".")
            os.replace(tmp_tar, out_tar)
        finally:
            if tmp_tar.exists():
                tmp_tar.unlink()

        print(f"[OK] Converted {converted_count} segment(s) -> {out_tar}")
        return converted_count


def step_convert(cfg: dict, dry_run: bool = False) -> None:
    loc = cfg.get("local") or {}
    work_dir = Path(loc.get("work_dir", "./work"))
    conv_cfg = dict(cfg.get("conversion") or {})
    if conv_cfg.get("bump_version", False):
        conv_cfg["_bumped_version"] = utc_now_iso()
    uuid_map = _validate_uuid_map(conv_cfg.get("uuid_map"))
    ds_regex_str = str(conv_cfg.get("datasource_regex") or "").strip()
    start_dt = _parse_iso_utc(conv_cfg.get("start_date"), "conversion.start_date", is_end=False)
    stop_dt = _parse_iso_utc(conv_cfg.get("stop_date"), "conversion.stop_date", is_end=True)
    if start_dt and stop_dt and start_dt > stop_dt:
        raise ValueError(
            f"conversion.start_date ({conv_cfg.get('start_date')!r}) must not be after conversion.stop_date ({conv_cfg.get('stop_date')!r})"
        )

    ds_pattern = None
    if ds_regex_str:
        try:
            ds_pattern = re.compile(ds_regex_str)
        except re.error as exc:
            raise ValueError(
                f"Invalid conversion.datasource_regex {ds_regex_str!r}: {exc}"
            ) from exc

    targets = resolve_convert_targets(cfg, dry_run=dry_run)
    total_converted = 0
    for raw_tar, out_tar in targets:
        total_converted += _convert_single_archive(
            raw_tar,
            out_tar,
            conv_cfg,
            uuid_map,
            ds_pattern,
            start_dt,
            stop_dt,
            dry_run=dry_run,
            work_dir=work_dir,
        )
    if isinstance(cfg.get("local"), dict):
        cfg["local"]["_convert_skipped_all"] = total_converted == 0


def resolve_converted_batches(cfg: dict) -> list:
    loc = cfg.get("local") or {}
    ng = cfg.get("ng") or {}
    work_dir = Path(loc.get("work_dir", "./work"))
    raw_name = _raw_tar_filename(loc)
    out_name = _converted_tar_filename(loc)
    remote_base = Path(ng["remote_tar_path"])
    raw_run_id = loc.get("run_dir") if loc.get("run_dir") is not None else loc.get("_active_run_dir")
    run_id = _validate_run_id(raw_run_id) if raw_run_id is not None else ""

    if run_id:
        return [
            (
                run_id,
                work_dir / run_id / out_name,
                str(remote_base.parent / run_id / remote_base.name),
            )
        ]

    if work_dir.is_dir():
        converted_dirs = [
            d for d in sorted(work_dir.iterdir()) if d.is_dir() and (d / out_name).is_file()
        ]
        if converted_dirs:
            return [
                (
                    d.name,
                    d / out_name,
                    str(remote_base.parent / d.name / remote_base.name),
                )
                for d in converted_dirs
            ]
        raw_only_dirs = [
            d for d in sorted(work_dir.iterdir()) if d.is_dir() and (d / raw_name).is_file()
        ]
        if raw_only_dirs:
            d = raw_only_dirs[0]
            return [
                (
                    d.name,
                    d / out_name,
                    str(remote_base.parent / d.name / remote_base.name),
                )
            ]

    return [(None, Path(loc["converted_tar_path"]), str(ng["remote_tar_path"]))]


def step_send_tar(cfg: dict, dry_run: bool = False) -> None:
    ng = cfg["ng"]
    batches = resolve_converted_batches(cfg)
    for _, out_tar, remote_tar in batches:
        if not dry_run and not out_tar.is_file():
            raise FileNotFoundError(
                f"Converted archive not found: {out_tar} (run 'convert' step first)"
            )
        remote_dir = str(Path(remote_tar).parent)
        mkdir_cmd = _ssh_prefix(ng) + [f"bash -lc {shlex.quote(f'mkdir -p {remote_dir}')}"]
        run_cmd(mkdir_cmd, dry_run=dry_run)
        run_cmd(build_send_tar_cmd(cfg, local_tar=out_tar, remote_tar=remote_tar), dry_run=dry_run)


def step_import(cfg: dict, dry_run: bool = False) -> None:
    batches = resolve_converted_batches(cfg)
    for _, out_tar, remote_tar in batches:
        if not dry_run and not out_tar.is_file():
            raise FileNotFoundError(
                f"Converted archive not found: {out_tar} (run 'convert' step first)"
            )
        run_cmd(build_import_cmd(cfg, remote_tar=remote_tar), dry_run=dry_run)


def step_clean(cfg: dict, dry_run: bool = False) -> None:
    loc = cfg.get("local") or {}
    work_dir_str = str(loc.get("work_dir") or "").strip()
    if not work_dir_str:
        raise ValueError("local.work_dir must not be empty")
    work_dir = Path(work_dir_str)
    resolved_work = work_dir.resolve()
    if resolved_work in (Path("/"), Path.cwd().resolve()):
        raise ValueError(f"Refusing to clean unsafe work_dir: {work_dir}")

    raw_run_id = loc.get("run_dir")
    if raw_run_id is not None:
        run_id = _validate_run_id(raw_run_id)
        target = work_dir / run_id
        if dry_run:
            print(f"[DRY-RUN] Clean {target}")
            return
        if target.is_symlink() or target.is_file():
            target.unlink()
        elif target.is_dir():
            shutil.rmtree(target)
        print(f"[OK] Cleaned {target}")
        return

    if dry_run:
        print(f"[DRY-RUN] Clean contents of {work_dir}")
        return
    if work_dir.is_dir():
        for entry in sorted(work_dir.iterdir()):
            if entry.is_symlink() or entry.is_file():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry)
    print(f"[OK] Cleaned contents of {work_dir}")


def step_verify(cfg: dict, dry_run: bool = False) -> None:
    run_cmd(build_verify_cmd(cfg), dry_run=dry_run)


SAFE_SEGMENT_TOKEN_RE = re.compile(r"^[A-Za-z0-9._:-]+$")


def _compile_segment_filters(
    datasource_regex: str = None,
    start_date: str = None,
    stop_date: str = None,
):
    ds_regex_str = str(datasource_regex).strip() if datasource_regex is not None else ""
    ds_pattern = None
    if ds_regex_str:
        try:
            ds_pattern = re.compile(ds_regex_str)
        except re.error as exc:
            raise ValueError(f"Invalid datasource_regex {datasource_regex!r}: {exc}") from exc

    start_dt = _parse_iso_utc(start_date, "start_date", is_end=False) if start_date else None
    stop_dt = _parse_iso_utc(stop_date, "stop_date", is_end=True) if stop_date else None
    if start_dt is not None and stop_dt is not None and start_dt > stop_dt:
        raise ValueError(
            f"conversion.start_date ({start_date}) must not be after conversion.stop_date ({stop_date})"
        )
    return ds_pattern, start_dt, stop_dt


def _segment_matches_filters(
    datasource: str,
    start_iso: str,
    end_iso: str,
    ds_pattern,
    start_dt: datetime = None,
    stop_dt: datetime = None,
) -> bool:
    if ds_pattern is not None and not ds_pattern.search(datasource):
        return False
    if start_dt is not None or stop_dt is not None:
        seg_start_dt = _parse_iso_utc(start_iso, "segment start")
        seg_end_dt = _parse_iso_utc(end_iso, "segment end")
        if start_dt is not None and seg_start_dt < start_dt:
            return False
        if stop_dt is not None and seg_end_dt > stop_dt:
            return False
    return True


def _validate_safe_segment_token(value: str, field_name: str) -> str:
    if value is None:
        raise ValueError(f"Unsafe segment {field_name}: {value!r}")
    s = str(value).strip()
    if not s or ".." in s or not SAFE_SEGMENT_TOKEN_RE.match(s):
        raise ValueError(f"Unsafe segment {field_name}: {value!r}")
    return s


def collect_segments_from_converted_tar(
    tar_path: Path,
    datasource_regex: str = None,
    start_date: str = None,
    stop_date: str = None,
) -> list:
    ds_pattern, start_dt, stop_dt = _compile_segment_filters(
        datasource_regex=datasource_regex,
        start_date=start_date,
        stop_date=stop_date,
    )
    segments = []
    with tarfile.open(tar_path, "r:*") as tf:
        for member in sorted(tf.getmembers(), key=lambda m: m.name):
            if not member.isfile():
                continue
            norm_name = member.name.lstrip("./")
            parts = Path(norm_name).parts
            if len(parts) == 6 and parts[0] == "segments" and parts[5] == "rule.json":
                _, ds, interval_dir, version, part, _ = parts
                fobj = tf.extractfile(member)
                if fobj is None:
                    continue
                rule = json.loads(fobj.read().decode("utf-8"))
                seg_id = _validate_safe_segment_token(rule["id"], "id")
                seg_ds = _validate_safe_segment_token(rule.get("datasource", ds), "datasource")
                seg_start = _validate_safe_segment_token(rule["start"], "start")
                seg_end = _validate_safe_segment_token(rule["end"], "end")
                seg_ver = _validate_safe_segment_token(rule.get("version", version), "version")
                seg_part = _validate_safe_segment_token(rule.get("partition", part), "partition")
                seg_interval = _validate_safe_segment_token(interval_dir, "interval")
                if not _segment_matches_filters(
                    seg_ds, seg_start, seg_end, ds_pattern, start_dt, stop_dt
                ):
                    continue
                segments.append(
                    {
                        "id": seg_id,
                        "datasource": seg_ds,
                        "start": seg_start,
                        "end": seg_end,
                        "interval": seg_interval,
                        "version": seg_ver,
                        "partition": seg_part,
                        "s3_rel_dir": f"{seg_ds}/{seg_interval}/{seg_ver}/{seg_part}",
                    }
                )
    return segments


def parse_pg_segments_tsv(
    tsv_output: str,
    datasource_regex: str = None,
    start_date: str = None,
    stop_date: str = None,
) -> list:
    ds_pattern, start_dt, stop_dt = _compile_segment_filters(
        datasource_regex=datasource_regex,
        start_date=start_date,
        stop_date=stop_date,
    )
    segments = []
    for raw_line in (tsv_output or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cols = line.split("\t")
        if len(cols) != 6:
            continue
        seg_id, ds, seg_start, seg_end, version, part = [
            _validate_safe_segment_token(c, "pg_column") for c in cols
        ]
        if not _segment_matches_filters(ds, seg_start, seg_end, ds_pattern, start_dt, stop_dt):
            continue
        interval_dir = f"{seg_start}_{seg_end}"
        segments.append(
            {
                "id": seg_id,
                "datasource": ds,
                "start": seg_start,
                "end": seg_end,
                "interval": interval_dir,
                "version": version,
                "partition": part,
                "s3_rel_dir": f"{ds}/{interval_dir}/{version}/{part}",
            }
        )
    return segments


def _sql_literal(val: str) -> str:
    return "'" + str(val).replace("'", "''") + "'"


def build_rollback_remote_cmd(cfg: dict) -> list:
    ng = cfg["ng"]
    return _ssh_prefix(ng) + ["bash -s"]


def build_rollback_remote_script(cfg: dict, segments: list) -> str:
    if not segments:
        raise ValueError("No segments provided to build_rollback_remote_script")
    conv = cfg.get("conversion") or {}
    bucket = _validate_safe_segment_token(conv.get("target_bucket") or "bucket", "target_bucket")
    base_key = _validate_safe_segment_token(
        conv.get("target_base_key") or "rbdata", "target_base_key"
    )

    id_literals = ", ".join(_sql_literal(seg["id"]) for seg in segments)
    sql_statements = [
        r"\set ON_ERROR_STOP on",
        f"DELETE FROM druid_segments WHERE id IN ({id_literals});",
    ]

    seen_intervals = []
    for seg in segments:
        key = (seg["datasource"], seg["start"], seg["end"])
        if key not in seen_intervals:
            seen_intervals.append(key)

    now_lit = _sql_literal(utc_now_iso())
    for ds, seg_start, seg_end in seen_intervals:
        ds_lit = _sql_literal(ds)
        start_lit = _sql_literal(seg_start)
        end_lit = _sql_literal(seg_end)
        sql_statements.append(
            f"UPDATE druid_segments SET used = true, used_status_last_updated = {now_lit} "
            f'WHERE datasource = {ds_lit} AND start = {start_lit} AND "end" = {end_lit} '
            f"AND version = ("
            f"SELECT MAX(version) FROM druid_segments "
            f'WHERE datasource = {ds_lit} AND start = {start_lit} AND "end" = {end_lit}'
            f") AND NOT EXISTS ("
            f"SELECT 1 FROM druid_segments "
            f'WHERE datasource = {ds_lit} AND start = {start_lit} AND "end" = {end_lit} AND used = true'
            f");"
        )

    sql_script = "\n".join(sql_statements)

    mcli_rel_paths = []
    cache_paths = []
    for seg in segments:
        rel_dir = seg["s3_rel_dir"]
        seg_id = seg["id"]
        mcli_rel_paths.append(f"{bucket}/{base_key}/{rel_dir}/")
        cache_paths.extend(
            [
                f"/var/druid/historical/indexCache/{rel_dir}",
                f"/var/druid/historical/indexCache/info_dir/{seg_id}",
                f"/var/druid/data/{rel_dir}",
            ]
        )

    mcli_lines = "\n".join(mcli_rel_paths)
    cache_lines = "\n".join(cache_paths)

    script_lines = [
        "set -euo pipefail",
        'export PATH="/usr/lib/redborder/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:$PATH"',
        'HOST_SHORT="$(hostname -s)"',
        "rb_psql druid <<'__RB_SQL_EOF__'",
        sql_script,
        "__RB_SQL_EOF__",
        "while IFS= read -r rel_path; do",
        '  [ -n "$rel_path" ] && printf \'%s/%s\\n\' "$HOST_SHORT" "$rel_path"',
        "done <<'__RB_MCLI_EOF__' | xargs -r -n 50 /usr/local/bin/mcli rm --recursive --force >/dev/null 2>&1 || true",
        mcli_lines,
        "__RB_MCLI_EOF__",
        "xargs -r -n 100 rm -rf <<'__RB_CACHE_EOF__'",
        cache_lines,
        "__RB_CACHE_EOF__",
    ]
    return "\n".join(script_lines) + "\n"


def build_ng_segments_query_sql() -> str:
    return (
        'COPY (SELECT id, datasource, start, "end", version, '
        "COALESCE(substring(id from 'Z_([0-9]+)$'), '0') AS partition "
        "FROM druid_segments ORDER BY datasource, start) TO STDOUT;"
    )


def step_rollback(cfg: dict, dry_run: bool = False) -> None:
    loc = cfg.get("local") or {}
    conv = cfg.get("conversion") or {}
    ng = cfg["ng"]

    raw_run_id = loc.get("run_dir")
    datasource_regex = conv.get("datasource_regex")
    start_date = conv.get("start_date")
    stop_date = conv.get("stop_date")

    # Validate filters upfront
    ds_pattern, start_dt, stop_dt = _compile_segment_filters(
        datasource_regex=datasource_regex,
        start_date=start_date,
        stop_date=stop_date,
    )

    if raw_run_id is None and ds_pattern is None and start_dt is None and stop_dt is None:
        raise ValueError(
            "Refusing to run rollback without --run-dir or at least one filter "
            "(--datasource-regex, --start-date, --stop-date)"
        )

    if raw_run_id is not None:
        run_id = _validate_run_id(raw_run_id)
        work_dir = Path(str(loc.get("work_dir", "./work")))
        out_tar = work_dir / run_id / _converted_tar_filename(loc)
        if not out_tar.is_file():
            if dry_run:
                print(f"[DRY-RUN] Rollback segments from {out_tar}")
                return
            raise FileNotFoundError(f"Converted archive not found for rollback: {out_tar}")
        segments = collect_segments_from_converted_tar(
            out_tar,
            datasource_regex=datasource_regex,
            start_date=start_date,
            stop_date=stop_date,
        )
    else:
        if dry_run:
            print(
                f"[DRY-RUN] Query NG druid_segments matching "
                f"datasource_regex={datasource_regex!r}, start_date={start_date!r}, stop_date={stop_date!r} "
                f"and rollback matching segments from PG, MinIO, and indexCache"
            )
            return
        query_sql = build_ng_segments_query_sql()
        query_script = (
            "set -euo pipefail\n"
            'export PATH="/usr/lib/redborder/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:$PATH"\n'
            "rb_psql druid <<'__RB_QUERY_EOF__'\n"
            f"{query_sql}\n"
            "__RB_QUERY_EOF__\n"
        )
        query_cmd = _ssh_prefix(ng) + ["bash -s"]
        print(f"[EXEC] {' '.join(shlex.quote(str(x)) for x in query_cmd)}")
        env = os.environ.copy()
        env.setdefault("OPENSSL_ENABLE_SHA1_SIGNATURES", "1")
        res = subprocess.run(
            query_cmd, check=True, env=env, input=query_script, capture_output=True, text=True
        )
        segments = parse_pg_segments_tsv(
            res.stdout,
            datasource_regex=datasource_regex,
            start_date=start_date,
            stop_date=stop_date,
        )

    if not segments:
        print("[OK] No matching segments found to rollback.")
        return

    print(f"[INFO] Rolling back {len(segments)} segment(s) on NG...")
    script = build_rollback_remote_script(cfg, segments)
    run_cmd(build_rollback_remote_cmd(cfg), dry_run=dry_run, input_text=script)
    print(f"[OK] Rolled back {len(segments)} segment(s).")


STEPS = {
    "export": step_export,
    "get-tar": step_get_tar,
    "convert": step_convert,
    "send-tar": step_send_tar,
    "import": step_import,
    "verify": step_verify,
    "clean": step_clean,
    "rollback": step_rollback,
}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Config-driven Druid segment migration tool (Legacy 0.12.3 -> NG 31.0.1)"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to YAML configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands and actions without executing them",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Override legacy.filter (-g grep pattern passed to Legacy export script)",
    )
    parser.add_argument(
        "--datasource-regex",
        default=None,
        help="Override conversion.datasource_regex (Python regex to filter datasources during convert)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Override conversion.start_date (ISO-8601 minimum segment start timestamp during convert)",
    )
    parser.add_argument(
        "--stop-date",
        default=None,
        help="Override conversion.stop_date (ISO-8601 maximum segment end timestamp during convert)",
    )
    parser.add_argument(
        "--run-dir",
        default=None,
        help="Specific batch subdirectory name inside local.work_dir (e.g. '20261023_100000')",
    )
    parser.add_argument(
        "--bump-version",
        action="store_true",
        default=None,
        help="Bump converted segment version to current UTC timestamp so imported segments supersede existing NG segments",
    )
    parser.add_argument(
        "step",
        choices=list(STEPS.keys()) + ["all"],
        help="Migration step to execute",
    )
    return parser.parse_args(argv)


def apply_cli_overrides(cfg: dict, args: argparse.Namespace) -> None:
    if not isinstance(cfg.get("legacy"), dict):
        cfg["legacy"] = {}
    if not isinstance(cfg.get("local"), dict):
        cfg["local"] = {}
    if not isinstance(cfg.get("conversion"), dict):
        cfg["conversion"] = {}
    if getattr(args, "filter", None) is not None:
        cfg["legacy"]["filter"] = args.filter
    if getattr(args, "run_dir", None) is not None:
        cfg["local"]["run_dir"] = args.run_dir
    conv = cfg["conversion"]
    if getattr(args, "datasource_regex", None) is not None:
        conv["datasource_regex"] = args.datasource_regex
    if getattr(args, "start_date", None) is not None:
        conv["start_date"] = args.start_date
    if getattr(args, "stop_date", None) is not None:
        conv["stop_date"] = args.stop_date
    if getattr(args, "bump_version", None) is not None:
        conv["bump_version"] = bool(args.bump_version)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    apply_cli_overrides(cfg, args)
    if args.step == "all":
        for name in ["export", "get-tar", "convert", "send-tar", "import", "verify"]:
            print(f"=== Step: {name} ===")
            STEPS[name](cfg, dry_run=args.dry_run)
            if name == "convert" and cfg.get("local", {}).get("_convert_skipped_all"):
                print("[WARN] No segments were converted; skipping send-tar, import, and verify.")
                break
    else:
        STEPS[args.step](cfg, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())

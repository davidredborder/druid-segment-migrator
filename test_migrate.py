import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
import warnings
import zipfile

import migrate


SAMPLE_DESCRIPTOR = {
    "dataSource": "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0",
    "interval": "2026-10-22T18:00:00.000Z/2026-10-22T19:00:00.000Z",
    "version": "2026-10-22T18:00:00.000Z",
    "loadSpec": {
        "type": "s3_zip",
        "bucket": "redborder",
        "key": "rbdata/rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0/index.zip",
        "S3Schema": "s3n",
    },
    "dimensions": "application_id_name,client_mac",
    "metrics": "events,sum_bytes",
    "shardSpec": {"type": "linear", "partitionNum": 0},
    "binaryVersion": 9,
    "size": 3420578,
    "identifier": "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0_2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z_2026-10-22T18:00:00.000Z",
}

SAMPLE_LEGACY_RULE = {
    "id": "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0_2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z_2026-10-22T18:00:00.000Z",
    "datasource": "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0",
    "created_date": "2026-10-22T19:30:00.123Z",
    "start": "2026-10-22T18:00:00.000Z",
    "end": "2026-10-22T19:00:00.000Z",
    "partitioned": "t",
    "version": "2026-10-22T18:00:00.000Z",
    "used": "f",
    "payload": dict(SAMPLE_DESCRIPTOR),
}


def _safe_extractall(tf: tarfile.TarFile, dest: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        if hasattr(tarfile, "data_filter"):
            tf.extractall(dest, filter="data")
        else:
            tf.extractall(dest)


class TestMigrate(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="druid_mig_test_"))
        self.raw_tar = self.tmpdir / "legacy-segments.tar.gz"
        self.out_tar = self.tmpdir / "ng-segments.tar"
        self.config = {
            "legacy": {
                "host": "10.1.33.48",
                "port": 22,
                "user": "root",
                "password": "redborder",
                "ssh_extra_args": [
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "HostKeyAlgorithms=+ssh-rsa",
                    "-o",
                    "PubkeyAcceptedKeyTypes=+ssh-rsa",
                ],
                "backup_script": "/opt/rb/bin/rb_backup_segments.sh",
                "remote_tar_path": "/tmp/legacy-segments.tar.gz",
                "filter": "rb_flow",
            },
            "local": {
                "work_dir": str(self.tmpdir / "work"),
                "raw_tar_path": str(self.raw_tar),
                "converted_tar_path": str(self.out_tar),
            },
            "conversion": {
                "target_bucket": "bucket",
                "target_base_key": "rbdata",
                "enable_segments": True,
                "datasource_map": {},
                "uuid_map": {},
                "datasource_regex": "",
                "start_date": "",
                "stop_date": "",
            },
            "ng": {
                "host": "10.1.33.160",
                "port": 22,
                "user": "root",
                "password": "redborder",
                "ssh_extra_args": ["-o", "StrictHostKeyChecking=no"],
                "import_script": "/usr/lib/redborder/bin/rb_export_import_segments.sh",
                "remote_tar_path": "/var/backup/segments/ng-segments.tar",
                "enable_on_import": True,
                "restore_full_db": False,
            },
        }

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _build_legacy_tar(self, include_rule=True, empty_rule=False, extra_Subdir=False, partition="0"):
        src_dir = self.tmpdir / "legacy_src"
        if src_dir.exists():
            shutil.rmtree(src_dir)
        (src_dir / "conf").mkdir(parents=True)
        (src_dir / "conf" / "db-druid-dump.psql").write_bytes(b"PGDMP_DUMMY")

        base_seg = src_dir / "segments" / "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0"
        if extra_Subdir:
            # Simulate reindexed segment with <ds>/<ds>/<interval>/<ver>/<part>
            base_seg = base_seg / "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0"

        seg_dir = (
            base_seg
            / "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z"
            / "2026-10-22T18:00:00.000Z"
            / str(partition)
        )
        seg_dir.mkdir(parents=True)
        shutil.copy(Path(__file__).resolve().parent / "legacy" / "index.zip", seg_dir / "index.zip")
        (seg_dir / "descriptor.json").write_text(json.dumps(SAMPLE_DESCRIPTOR))
        if include_rule:
            if empty_rule:
                (seg_dir / "rule.json").write_text("")
            else:
                (seg_dir / "rule.json").write_text(json.dumps(SAMPLE_LEGACY_RULE, indent=2))

        with tarfile.open(self.raw_tar, "w:gz") as tf:
            tf.add(src_dir, arcname=".")

    def test_convert_transforms_rule_and_removes_descriptor(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        migrate.step_convert(self.config, dry_run=False)

        self.assertTrue(self.out_tar.exists())
        with open(self.out_tar, "rb") as f:
            self.assertEqual(f.read(2), b"\x1f\x8b")

        inspect_dir = self.tmpdir / "inspect"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)

        self.assertTrue((inspect_dir / "conf" / "db-druid-dump.psql").is_file())
        seg_dir = (
            inspect_dir
            / "segments"
            / "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0"
            / "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z"
            / "2026-10-22T18:00:00.000Z"
            / "0"
        )
        self.assertTrue((seg_dir / "index.zip").is_file())
        self.assertFalse((seg_dir / "descriptor.json").exists())
        self.assertTrue((seg_dir / "rule.json").is_file())

        rule = json.loads((seg_dir / "rule.json").read_text())
        self.assertEqual(rule["used"], "t")
        self.assertEqual(rule["used_status_last_updated"], "2026-10-22T19:30:00.123Z")
        self.assertEqual(rule["payload"]["loadSpec"]["bucket"], "bucket")
        self.assertEqual(
            rule["payload"]["loadSpec"]["key"],
            "rbdata/rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0/index.zip",
        )

    def test_convert_synthesizes_rule_when_empty_and_maps_datasource(self):
        self._build_legacy_tar(include_rule=True, empty_rule=True, extra_Subdir=True, partition="2")
        self.config["conversion"]["datasource_map"] = {
            "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0": "rb_flow_f5e59c93-6053-453a-9362-3f5ea6af9fa2"
        }
        self.config["conversion"]["enable_segments"] = False
        migrate.step_convert(self.config, dry_run=False)

        inspect_dir = self.tmpdir / "inspect_mapped"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)

        new_ds = "rb_flow_f5e59c93-6053-453a-9362-3f5ea6af9fa2"
        seg_dir = (
            inspect_dir
            / "segments"
            / new_ds
            / "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z"
            / "2026-10-22T18:00:00.000Z"
            / "2"
        )
        self.assertTrue((seg_dir / "index.zip").is_file())
        rule = json.loads((seg_dir / "rule.json").read_text())
        self.assertEqual(rule["datasource"], new_ds)
        self.assertEqual(rule["used"], "f")
        self.assertEqual(rule["payload"]["dataSource"], new_ds)
        self.assertEqual(rule["payload"]["shardSpec"]["partitionNum"], 2)
        expected_id = f"{new_ds}_2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z_2026-10-22T18:00:00.000Z_2"
        self.assertEqual(rule["id"], expected_id)
        self.assertEqual(rule["payload"]["identifier"], expected_id)
        self.assertEqual(rule["payload"]["loadSpec"]["bucket"], "bucket")
        self.assertEqual(
            rule["payload"]["loadSpec"]["key"],
            f"rbdata/{new_ds}/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/2/index.zip",
        )
        self.assertTrue(rule["used_status_last_updated"].endswith("Z"))

    def test_date_filtering_and_send_tar_missing_file_check(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        self.config["conversion"]["start_date"] = "2026-10-23T00:00:00.000Z"
        # When 0 segments match filters, step_convert warns and skips creating out_tar without raising
        migrate.step_convert(self.config, dry_run=False)
        self.assertFalse(self.out_tar.exists())

        # When legacy-segments.tar.gz inside a batch run_dir has an empty segments/ dir (not indexed yet),
        # step_convert warns, skips creating out_tar, and cleans up the empty batch dir
        work_dir = Path(self.config["local"]["work_dir"])
        empty_run = work_dir / "20261023_121014"
        empty_run.mkdir(parents=True)
        empty_src = self.tmpdir / "empty_legacy_src"
        (empty_src / "conf").mkdir(parents=True)
        (empty_src / "conf" / "db-druid-dump.psql").write_bytes(b"PGDMP_DUMMY")
        (empty_src / "segments").mkdir(parents=True)
        empty_raw = empty_run / "legacy-segments.tar.gz"
        with tarfile.open(empty_raw, "w:gz") as tf:
            tf.add(empty_src, arcname=".")
        self.raw_tar.unlink()
        self.config["conversion"]["start_date"] = ""
        migrate.step_convert(self.config, dry_run=False)
        self.assertFalse(empty_run.exists())
        self.assertTrue(work_dir.exists())
        self.assertTrue(self.config["local"].get("_convert_skipped_all"))

        # Flat raw_tar inside work_dir (not in a <run_id> subdir) is NOT deleted when empty
        flat_raw = work_dir / "legacy-segments.tar.gz"
        with tarfile.open(flat_raw, "w:gz") as tf:
            tf.add(empty_src, arcname=".")
        self.config["local"]["raw_tar_path"] = str(flat_raw)
        migrate.step_convert(self.config, dry_run=False)
        self.assertTrue(flat_raw.is_file())
        flat_raw.unlink()
        self.config["local"]["raw_tar_path"] = str(self.raw_tar)
        self.config["local"].pop("_convert_skipped_all", None)

        # send-tar fails fast if converted_tar_path does not exist
        with self.assertRaises(FileNotFoundError):
            migrate.step_send_tar(self.config, dry_run=False)

    def test_command_builders(self):
        export_cmd = migrate.build_export_cmd(self.config)
        self.assertIn(
            "/opt/rb/bin/rb_backup_segments.sh -n -f /tmp/legacy-segments.tar.gz -g rb_flow",
            " ".join(export_cmd),
        )

        import_cmd = migrate.build_import_cmd(self.config)
        self.assertIn(
            "/usr/lib/redborder/bin/rb_export_import_segments.sh -r -n -v -e -f /var/backup/segments/ng-segments.tar",
            " ".join(import_cmd),
        )
        self.assertNotIn(" -b", " ".join(import_cmd))

    def _build_multi_segment_legacy_tar(self):
        src_dir = self.tmpdir / "legacy_multi_src"
        if src_dir.exists():
            shutil.rmtree(src_dir)
        (src_dir / "conf").mkdir(parents=True)
        (src_dir / "conf" / "db-druid-dump.psql").write_bytes(b"PGDMP_DUMMY")

        segments = [
            ("rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0", "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z", "2026-10-22T18:00:00.000Z"),
            ("rb_event", "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z", "2026-10-22T18:00:00.000Z"),
            ("rb_event", "2026-10-22T20:00:00.000Z_2026-10-22T21:00:00.000Z", "2026-10-22T20:00:00.000Z"),
            ("rb_state", "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z", "2026-10-22T18:00:00.000Z"),
        ]
        for ds, interval_dir, ver in segments:
            seg_dir = src_dir / "segments" / ds / interval_dir / ver / "0"
            seg_dir.mkdir(parents=True)
            shutil.copy(Path(__file__).resolve().parent / "legacy" / "index.zip", seg_dir / "index.zip")
            desc = dict(SAMPLE_DESCRIPTOR)
            desc["dataSource"] = ds
            desc["interval"] = interval_dir.replace("_", "/")
            desc["version"] = ver
            desc["identifier"] = f"{ds}_{interval_dir}_{ver}"
            (seg_dir / "descriptor.json").write_text(json.dumps(desc))

        with tarfile.open(self.raw_tar, "w:gz") as tf:
            tf.add(src_dir, arcname=".")

    def test_datasource_regex_and_stop_date_filtering(self):
        self._build_multi_segment_legacy_tar()
        self.config["conversion"]["datasource_regex"] = r"^(rb_flow_.*|rb_event)$"
        # Use ISO-8601 without milliseconds (.000) to verify timestamp parsing normalization
        self.config["conversion"]["start_date"] = "2026-10-22T18:00:00Z"
        self.config["conversion"]["stop_date"] = "2026-10-22T19:00:00Z"
        migrate.step_convert(self.config, dry_run=False)

        inspect_dir = self.tmpdir / "inspect_multi"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)

        zips = sorted(
            str(p.relative_to(inspect_dir / "segments"))
            for p in (inspect_dir / "segments").rglob("index.zip")
        )
        self.assertEqual(
            zips,
            [
                "rb_event/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0/index.zip",
                "rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0/index.zip",
            ],
        )

    def test_invalid_datasource_regex_or_dates_raise_value_error(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        self.config["conversion"]["datasource_regex"] = "[unclosed"
        with self.assertRaises(ValueError):
            migrate.step_convert(self.config, dry_run=False)

        self.config["conversion"]["datasource_regex"] = ""
        self.config["conversion"]["start_date"] = "not-a-date"
        with self.assertRaises(ValueError):
            migrate.step_convert(self.config, dry_run=False)

    def test_cli_overrides_filter_datasource_regex_and_dates(self):
        self._build_multi_segment_legacy_tar()
        cfg_path = self.tmpdir / "test_config.yaml"
        cfg_path.write_text(migrate.yaml.safe_dump(self.config), encoding="utf-8")

        args = migrate.parse_args(
            [
                "-c",
                str(cfg_path),
                "--filter",
                r"rb_event\|rb_state",
                "--datasource-regex",
                "^rb_event$",
                "--start-date",
                "2026-10-22T18:00:00Z",
                "--stop-date",
                "2026-10-22T19:00:00Z",
                "convert",
            ]
        )
        migrate.apply_cli_overrides(self.config, args)
        self.assertEqual(self.config["legacy"]["filter"], r"rb_event\|rb_state")
        self.assertEqual(self.config["conversion"]["datasource_regex"], "^rb_event$")
        self.assertEqual(self.config["conversion"]["start_date"], "2026-10-22T18:00:00Z")
        self.assertEqual(self.config["conversion"]["stop_date"], "2026-10-22T19:00:00Z")

        rc = migrate.main(
            [
                "-c",
                str(cfg_path),
                "--datasource-regex",
                "^rb_event$",
                "--start-date",
                "2026-10-22T18:00:00Z",
                "--stop-date",
                "2026-10-22T19:00:00Z",
                "convert",
            ]
        )
        self.assertEqual(rc, 0)
        inspect_dir = self.tmpdir / "inspect_cli_main"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)
        zips = sorted(
            str(p.relative_to(inspect_dir / "segments"))
            for p in (inspect_dir / "segments").rglob("index.zip")
        )
        self.assertEqual(
            zips,
            [
                "rb_event/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0/index.zip",
            ],
        )

    def test_uuid_map_rewrites_index_zip_smoosh_and_datasource_suffix(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        uuid_map = {
            "78B73529-63A4-4B1F-9D45-3E48F0EB0DB0": "7c3c62f1-728f-41e5-aa69-e44bcd8830fa",
            "1199e7b3-1f84-4ade-bd26-44026b11291d": "2C07EAD9-6198-4F36-BB67-6BEEFF35C5C7",
            "eaaffc57-804a-4c30-b86a-39d3ce9597a9": "377a3c45-a530-4896-af58-16c0e7887974",
            "5dad542b-870b-4aeb-bc91-fbefe43af191": "d3510093-1cd1-4fa2-bf00-1c6c5f3e2f59",
        }
        self.config["conversion"]["uuid_map"] = uuid_map
        migrate.step_convert(self.config, dry_run=False)

        inspect_dir = self.tmpdir / "inspect_uuid_map"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)

        new_ds = "rb_flow_7c3c62f1-728f-41e5-aa69-e44bcd8830fa"
        seg_dir = (
            inspect_dir
            / "segments"
            / new_ds
            / "2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z"
            / "2026-10-22T18:00:00.000Z"
            / "0"
        )
        self.assertTrue((seg_dir / "index.zip").is_file())
        rule = json.loads((seg_dir / "rule.json").read_text(encoding="utf-8"))
        self.assertEqual(rule["datasource"], new_ds)
        self.assertEqual(rule["payload"]["dataSource"], new_ds)
        self.assertIn(new_ds, rule["id"])
        self.assertIn(new_ds, rule["payload"]["loadSpec"]["key"])

        with zipfile.ZipFile(Path(__file__).resolve().parent / "legacy" / "index.zip", "r") as orig_zf:
            orig_smoosh_len = len(orig_zf.read("00000.smoosh"))

        with zipfile.ZipFile(seg_dir / "index.zip", "r") as out_zf:
            self.assertEqual(
                sorted(out_zf.namelist()),
                ["00000.smoosh", "factory.json", "meta.smoosh", "version.bin"],
            )
            smoosh_bytes = out_zf.read("00000.smoosh")
            self.assertEqual(len(smoosh_bytes), orig_smoosh_len)
            for old_u, new_u in uuid_map.items():
                self.assertNotIn(old_u.lower().encode("ascii"), smoosh_bytes)
                self.assertIn(new_u.lower().encode("ascii"), smoosh_bytes)
            # Also verify non-UUID string columns (like sensor_name='router') remain untouched
            self.assertIn(b"router", smoosh_bytes)

    def test_invalid_uuid_map_raises_value_error(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        self.config["conversion"]["uuid_map"] = {
            "invalid-uuid": "7c3c62f1-728f-41e5-aa69-e44bcd8830fa"
        }
        with self.assertRaises(ValueError):
            migrate.step_convert(self.config, dry_run=False)

    def test_multi_batch_run_dirs_no_overwrite_and_clean(self):
        work_dir = Path(self.config["local"]["work_dir"])
        run1 = work_dir / "20261023_100000"
        run2 = work_dir / "20261023_100500"
        run1.mkdir(parents=True)
        run2.mkdir(parents=True)

        self._build_legacy_tar(include_rule=True, empty_rule=False)
        shutil.copy2(self.raw_tar, run1 / "legacy-segments.tar.gz")
        shutil.copy2(self.raw_tar, run2 / "legacy-segments.tar.gz")
        self.raw_tar.unlink()

        # 1. Convert both pending run directories without overwriting
        migrate.step_convert(self.config, dry_run=False)
        self.assertTrue((run1 / "ng-segments.tar").is_file())
        self.assertTrue((run2 / "ng-segments.tar").is_file())

        # 2. Running convert again when all run dirs are already converted refuses to overwrite
        with self.assertRaises(FileExistsError):
            migrate.step_convert(self.config, dry_run=False)

        # 3. Running get-tar into an existing run_dir with legacy-segments.tar.gz refuses to overwrite
        self.config["local"]["run_dir"] = "20261023_100000"
        with self.assertRaises(FileExistsError):
            migrate.step_get_tar(self.config, dry_run=False)
        self.config["local"].pop("run_dir", None)

        # 4. send-tar and import iterate over both run directories in order (dry_run check)
        batches = migrate.resolve_converted_batches(self.config)
        self.assertEqual(
            batches,
            [
                ("20261023_100000", run1 / "ng-segments.tar", "/var/backup/segments/20261023_100000/ng-segments.tar"),
                ("20261023_100500", run2 / "ng-segments.tar", "/var/backup/segments/20261023_100500/ng-segments.tar"),
            ],
        )

        # 5. Reject unsafe path-traversal --run-dir values
        for bad_run_dir in ("../escape", "/tmp/abs", "..", "a/b", ""):
            self.config["local"]["run_dir"] = bad_run_dir
            with self.assertRaises(ValueError):
                migrate.step_clean(self.config, dry_run=False)

        # 6. Clean a single run_dir first, then clean the rest of work_dir
        self.config["local"]["run_dir"] = "20261023_100000"
        migrate.step_clean(self.config, dry_run=False)
        self.assertFalse(run1.exists())
        self.assertTrue(run2.exists())

        self.config["local"].pop("run_dir", None)
        migrate.step_clean(self.config, dry_run=False)
        self.assertEqual(list(work_dir.iterdir()), [])

    def test_bump_version_updates_version_path_and_rule(self):
        self._build_legacy_tar(include_rule=True, empty_rule=False)
        args = migrate.parse_args(["--bump-version", "convert"])
        migrate.apply_cli_overrides(self.config, args)
        self.assertTrue(self.config["conversion"]["bump_version"])

        migrate.step_convert(self.config, dry_run=False)

        inspect_dir = self.tmpdir / "inspect_bump"
        with tarfile.open(self.out_tar, "r:gz") as tf:
            _safe_extractall(tf, inspect_dir)

        zips = list((inspect_dir / "segments").rglob("index.zip"))
        self.assertEqual(len(zips), 1)
        rel_parts = zips[0].relative_to(inspect_dir / "segments").parts
        ds, interval_dir, new_ver, part, _ = rel_parts
        self.assertGreater(new_ver, "2026-10-22T18:00:00.000Z")

        rule = json.loads((zips[0].parent / "rule.json").read_text(encoding="utf-8"))
        self.assertEqual(rule["version"], new_ver)
        self.assertEqual(rule["payload"]["version"], new_ver)
        self.assertTrue(rule["id"].endswith(f"_{new_ver}"))
        self.assertEqual(rule["payload"]["identifier"], rule["id"])
        self.assertEqual(
            rule["payload"]["loadSpec"]["key"],
            f"rbdata/{ds}/{interval_dir}/{new_ver}/{part}/index.zip",
        )

    def test_rollback_archive_and_filter_modes_and_safety_guard(self):
        # 1. CLI accepts 'rollback' step
        args = migrate.parse_args(["--run-dir", "20261023_100000", "rollback"])
        self.assertEqual(args.step, "rollback")
        self.assertEqual(args.run_dir, "20261023_100000")

        # 2. Safety guard: step_rollback without run_dir and without filters (or whitespace-only filters) must raise ValueError
        with self.assertRaises(ValueError):
            migrate.step_rollback(self.config, dry_run=True)

        self.config["conversion"]["datasource_regex"] = "   "
        self.config["conversion"]["start_date"] = "   "
        self.config["conversion"]["stop_date"] = "   "
        with self.assertRaises(ValueError):
            migrate.step_rollback(self.config, dry_run=True)
        self.config["conversion"]["datasource_regex"] = ""
        self.config["conversion"]["start_date"] = ""
        self.config["conversion"]["stop_date"] = ""

        # Verify NG query SQL extracts partition from id rather than non-existent partition column
        query_sql = migrate.build_ng_segments_query_sql()
        self.assertIn("COALESCE(substring(id from 'Z_([0-9]+)$'), '0')", query_sql)

        # 3. Build a multi-segment converted archive and test collect_segments_from_converted_tar
        self._build_multi_segment_legacy_tar()
        migrate.step_convert(self.config, dry_run=False)

        all_segs = migrate.collect_segments_from_converted_tar(self.out_tar)
        self.assertEqual(len(all_segs), 4)
        self.assertEqual(
            {s["datasource"] for s in all_segs},
            {"rb_flow_78b73529-63a4-4b1f-9d45-3e48f0eb0db0", "rb_event", "rb_state"},
        )

        # Narrow archive rollback by datasource_regex + start_date + stop_date
        filtered_segs = migrate.collect_segments_from_converted_tar(
            self.out_tar,
            datasource_regex=r"^rb_event$",
            start_date="2026-10-22T19:30:00.000Z",
            stop_date="2026-10-22T22:00:00.000Z",
        )
        self.assertEqual(len(filtered_segs), 1)
        self.assertEqual(filtered_segs[0]["datasource"], "rb_event")
        self.assertEqual(filtered_segs[0]["partition"], "0")
        self.assertEqual(
            filtered_segs[0]["id"],
            "rb_event_2026-10-22T20:00:00.000Z_2026-10-22T21:00:00.000Z_2026-10-22T20:00:00.000Z",
        )

        # 4. Test parse_pg_segments_tsv for filter-based NG query mode
        tsv_sample = "\n".join(
            [
                "rb_flow_2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z_2026-10-22T18:00:00.000Z\trb_flow\t2026-10-22T18:00:00.000Z\t2026-10-22T19:00:00.000Z\t2026-10-22T18:00:00.000Z\t0",
                "rb_monitor_2026-10-20T10:00:00.000Z_2026-10-20T11:00:00.000Z_2026-10-20T10:00:00.000Z\trb_monitor\t2026-10-20T10:00:00.000Z\t2026-10-20T11:00:00.000Z\t2026-10-20T10:00:00.000Z\t0",
                "rb_event_2026-10-23T08:00:00.000Z_2026-10-23T09:00:00.000Z_2026-10-23T08:00:00.000Z_1\trb_event\t2026-10-23T08:00:00.000Z\t2026-10-23T09:00:00.000Z\t2026-10-23T08:00:00.000Z\t1",
            ]
        )
        pg_segs = migrate.parse_pg_segments_tsv(
            tsv_sample,
            datasource_regex=r"^rb_flow$",
            start_date="2026-10-22T00:00:00Z",
            stop_date="2026-10-23T00:00:00Z",
        )
        self.assertEqual(len(pg_segs), 1)
        self.assertEqual(pg_segs[0]["datasource"], "rb_flow")
        self.assertEqual(
            pg_segs[0]["s3_rel_dir"],
            "rb_flow/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0",
        )

        # 5. Test build_rollback_remote_cmd + build_rollback_remote_script (stdin bash -s avoids MAX_ARG_STRLEN)
        cmd = migrate.build_rollback_remote_cmd(self.config)
        self.assertEqual(cmd[-1], "bash -s")
        script = migrate.build_rollback_remote_script(self.config, pg_segs)
        self.assertIn("set -euo pipefail", script)
        self.assertIn(r"\set ON_ERROR_STOP on", script)
        self.assertIn("DELETE FROM druid_segments WHERE id IN", script)
        self.assertIn("UPDATE druid_segments SET used = true, used_status_last_updated =", script)
        self.assertIn("/usr/local/bin/mcli rm --recursive --force", script)
        self.assertIn(
            "rbdata/rb_flow/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0",
            script,
        )
        self.assertIn("/var/druid/historical/indexCache/rb_flow/2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z/2026-10-22T18:00:00.000Z/0", script)
        self.assertIn("/var/druid/historical/indexCache/info_dir/rb_flow_2026-10-22T18:00:00.000Z_2026-10-22T19:00:00.000Z_2026-10-22T18:00:00.000Z", script)

        # Verify 300+ segments never bloat CLI args
        many_segs = [
            {
                "id": f"rb_monitor_78b73529-63a4-4b1f-9d45-3e48f0eb0db0_2026-10-20T00:00:00.000Z_2026-10-20T01:00:00.000Z_2026-10-20T00:00:00.000Z_{i}",
                "datasource": "rb_monitor_78b73529-63a4-4b1f-9d45-3e48f0eb0db0",
                "start": "2026-10-20T00:00:00.000Z",
                "end": "2026-10-20T01:00:00.000Z",
                "interval": "2026-10-20T00:00:00.000Z_2026-10-20T01:00:00.000Z",
                "version": "2026-10-20T00:00:00.000Z",
                "partition": str(i),
                "s3_rel_dir": f"rb_monitor_78b73529-63a4-4b1f-9d45-3e48f0eb0db0/2026-10-20T00:00:00.000Z_2026-10-20T01:00:00.000Z/2026-10-20T00:00:00.000Z/{i}",
            }
            for i in range(300)
        ]
        many_script = migrate.build_rollback_remote_script(self.config, many_segs)
        self.assertGreater(len(many_script), 50000)
        self.assertLess(sum(len(str(a)) for a in migrate.build_rollback_remote_cmd(self.config)), 500)

        # 6. Verify strict token validation rejects shell metacharacters and date-only stop_date works
        for bad_tok in ("$(whoami)", "a;b", '"quote"', "`id`", "a b", "../x"):
            with self.assertRaises(ValueError):
                migrate._validate_safe_segment_token(bad_tok, "test")

        day_segs = migrate.parse_pg_segments_tsv(
            tsv_sample,
            datasource_regex=r"^rb_flow$",
            start_date="2026-10-22",
            stop_date="2026-10-22",
        )
        self.assertEqual(len(day_segs), 1)


if __name__ == "__main__":
    unittest.main()

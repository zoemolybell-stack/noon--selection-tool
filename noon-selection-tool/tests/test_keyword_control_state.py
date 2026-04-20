from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops import keyword_control_state


class KeywordControlStateTests(unittest.TestCase):
    def test_baseline_roundtrip_persists_to_baseline_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            baseline_path = temp_root / "baseline.txt"
            baseline_path.write_text("# seeds\ndog toys\ncat litter\n", encoding="utf-8")
            config_path.write_text(
                json.dumps({"baseline_file": str(baseline_path)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                keyword_control_state.update_monitor_baseline_keywords(
                    str(config_path),
                    keywords=["foam roller"],
                    mode="add",
                )
                keyword_control_state.update_monitor_baseline_keywords(
                    str(config_path),
                    keywords=["dog toys"],
                    mode="remove",
                )

                payload = keyword_control_state.get_keyword_control_state(str(config_path))

            self.assertEqual(
                [item["keyword"] for item in payload["baseline_keywords"]],
                ["cat litter", "foam roller"],
            )
            self.assertEqual(payload["baseline_additions"], [])
            self.assertEqual(payload["baseline_removals"], [])
            self.assertEqual(payload["legacy_baseline_overlay_count"], 0)
            self.assertEqual(payload["baseline_file_keywords"], ["cat litter", "foam roller"])
            self.assertTrue(payload["baseline_file_writable"])
            self.assertEqual(
                baseline_path.read_text(encoding="utf-8").splitlines(),
                ["# seeds", "cat litter", "", "foam roller"],
            )

    def test_effective_baseline_keeps_legacy_sidecar_overlay_until_next_write(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            baseline_path = temp_root / "baseline.txt"
            baseline_path.write_text("dog toys\ncat litter\n", encoding="utf-8")
            config_path.write_text(
                json.dumps({"baseline_file": str(baseline_path)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                keyword_control_state._save_state(
                    str(config_path),
                    {
                        "baseline_additions": ["foam roller"],
                        "baseline_removals": ["dog toys"],
                        "exclusions": [],
                    },
                )
                payload = keyword_control_state.get_keyword_control_state(str(config_path))

            self.assertEqual(
                [item["keyword"] for item in payload["baseline_keywords"]],
                ["cat litter", "foam roller"],
            )
            self.assertEqual(payload["baseline_file_keywords"], ["dog toys", "cat litter"])
            self.assertEqual(payload["legacy_baseline_overlay_count"], 2)

    def test_exclusion_rules_match_by_keyword_and_scope(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            config_path.write_text("{}", encoding="utf-8")
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                payload = keyword_control_state.update_monitor_exclusion_rule(
                    str(config_path),
                    keyword="dog toys",
                    blocked_sources=["baseline", "tracked"],
                    reason="seed cleanup",
                    mode="upsert",
                )
                lookup = keyword_control_state.build_exclusion_lookup(payload)
                matched = keyword_control_state.match_exclusion_rule("dog toys", ["baseline"], lookup)
                skipped = keyword_control_state.match_exclusion_rule("dog toys", ["manual"], lookup)

            self.assertIsNotNone(matched)
            self.assertEqual(matched["blocked_sources"], ["baseline"])
            self.assertEqual(matched["reason"], "seed cleanup")
            self.assertIsNone(skipped)

    def test_exclusion_upsert_merges_sources_and_preserves_existing_reason(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            config_path.write_text("{}", encoding="utf-8")
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                keyword_control_state.update_monitor_exclusion_rule(
                    str(config_path),
                    keyword="dog toys",
                    blocked_sources=["baseline"],
                    reason="seed cleanup",
                    mode="upsert",
                )
                payload = keyword_control_state.update_monitor_exclusion_rule(
                    str(config_path),
                    keyword="dog toys",
                    blocked_sources=["tracked"],
                    reason="",
                    mode="upsert",
                )

            self.assertEqual(len(payload["exclusions"]), 1)
            self.assertEqual(payload["exclusions"][0]["blocked_sources"], ["baseline", "tracked"])
            self.assertEqual(payload["exclusions"][0]["reason"], "seed cleanup")

    def test_blocked_roots_support_exact_and_contains_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            config_path.write_text("{}", encoding="utf-8")
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                payload = keyword_control_state.update_monitor_blocked_root_rules(
                    str(config_path),
                    root_keywords=["adidas"],
                    blocked_sources=["generated", "tracked"],
                    reason="brand ignore",
                    match_mode="contains",
                    mode="upsert",
                )
                payload = keyword_control_state.update_monitor_blocked_root_rules(
                    str(config_path),
                    root_keywords=["nike"],
                    blocked_sources=["tracked"],
                    reason="exact seed ignore",
                    match_mode="exact",
                    mode="upsert",
                )
                lookup = keyword_control_state.build_exclusion_lookup(payload)
                contains_match = keyword_control_state.match_exclusion_rule("adidas running belt", ["generated"], lookup)
                exact_match = keyword_control_state.match_exclusion_rule("nike", ["tracked"], lookup)
                no_match = keyword_control_state.match_exclusion_rule("nike running belt", ["tracked"], lookup)

            self.assertIsNotNone(contains_match)
            self.assertEqual(contains_match["match_mode"], "contains")
            self.assertEqual(contains_match["keyword"], "adidas running belt")
            self.assertEqual(contains_match["root_keyword"], "adidas")
            self.assertIsNotNone(exact_match)
            self.assertEqual(exact_match["match_mode"], "exact")
            self.assertEqual(exact_match["keyword"], "nike")
            self.assertEqual(exact_match["root_keyword"], "nike")
            self.assertIsNone(no_match)

    def test_blocked_root_rules_match_exact_and_contains_candidates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            config_path.write_text("{}", encoding="utf-8")
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                sidecar_dir.mkdir(parents=True, exist_ok=True)
                payload = keyword_control_state.update_monitor_blocked_root_rules(
                    str(config_path),
                    root_keywords=["dog toys"],
                    blocked_sources=["generated"],
                    reason="root cleanup",
                    match_mode="exact",
                    mode="upsert",
                )
                payload = keyword_control_state.update_monitor_blocked_root_rules(
                    str(config_path),
                    root_keywords=["cat"],
                    blocked_sources=["manual"],
                    reason="contains cleanup",
                    match_mode="contains",
                    mode="upsert",
                )
                lookup = keyword_control_state.build_keyword_control_lookup(payload)
                exact_match = keyword_control_state.match_keyword_control_rule(
                    "dog shampoo",
                    ["generated"],
                    lookup,
                    root_keywords=["dog toys"],
                )
                contains_match = keyword_control_state.match_keyword_control_rule(
                    "cat bed premium",
                    ["manual"],
                    lookup,
                    root_keywords=["premium cat beds"],
                )
                skipped = keyword_control_state.match_keyword_control_rule(
                    "dog shampoo",
                    ["baseline"],
                    lookup,
                    root_keywords=["dog toys"],
                )

            self.assertEqual(payload["blocked_root_count"], 2)
            self.assertEqual(payload["available_root_match_modes"], ["exact", "contains"])
            self.assertEqual(payload["blocked_roots"][0]["root_keyword"], "dog toys")
            self.assertEqual(payload["blocked_roots"][1]["root_keyword"], "cat")
            self.assertEqual(exact_match["rule_type"], "blocked_root")
            self.assertEqual(exact_match["matched_keyword"], "dog toys")
            self.assertEqual(contains_match["match_mode"], "contains")
            self.assertEqual(contains_match["matched_keyword"], "premium cat beds")
            self.assertIsNone(skipped)


if __name__ == "__main__":
    unittest.main()

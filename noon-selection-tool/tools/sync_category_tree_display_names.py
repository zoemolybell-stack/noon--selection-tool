"""
将 runtime category map 中已经验证过的平台路径回写到 category_tree.json 的展示名。

目标：
- 让内部类目名称尽量与 Noon 前台 breadcrumb / category filter 一致
- 保持原有 id 不变，避免打断现有代码链路
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TREE = ROOT / "config" / "category_tree.json"
DEFAULT_RUNTIME_MAP = ROOT / "config" / "runtime_category_map.json"


def _index_nodes(category_tree: dict) -> tuple[dict[str, dict], dict[str, dict]]:
    parents: dict[str, dict] = {}
    leaves: dict[str, dict] = {}
    for category in category_tree.get("categories", []):
        for child in category.get("children", []):
            parents[child.get("id")] = child
            grandchildren = child.get("children", [])
            if grandchildren:
                for grandchild in grandchildren:
                    leaves[grandchild.get("id")] = grandchild
            else:
                leaves[child.get("id")] = child
    return parents, leaves


def sync_category_tree(category_tree_path: Path, runtime_map_path: Path) -> dict:
    category_tree = json.loads(category_tree_path.read_text(encoding="utf-8"))
    runtime_map = json.loads(runtime_map_path.read_text(encoding="utf-8"))

    parents, leaves = _index_nodes(category_tree)
    updates: list[dict] = []

    for category_bucket in runtime_map.get("categories", {}).values():
        for record in category_bucket.values():
            config_id = record.get("config_id")
            parent_config_id = record.get("parent_config_id")
            expected_path = record.get("expected_path") or []

            leaf = leaves.get(config_id)
            if leaf and expected_path:
                new_leaf_name = expected_path[-1]
                if new_leaf_name and leaf.get("name_en") != new_leaf_name:
                    updates.append(
                        {
                            "type": "leaf",
                            "config_id": config_id,
                            "old_name": leaf.get("name_en"),
                            "new_name": new_leaf_name,
                        }
                    )
                    leaf["name_en"] = new_leaf_name

            parent = parents.get(parent_config_id)
            if parent and len(expected_path) >= 2:
                new_parent_name = expected_path[-2]
                if new_parent_name and parent.get("name_en") != new_parent_name:
                    updates.append(
                        {
                            "type": "parent",
                            "config_id": parent_config_id,
                            "old_name": parent.get("name_en"),
                            "new_name": new_parent_name,
                        }
                    )
                    parent["name_en"] = new_parent_name

    category_tree_path.write_text(json.dumps(category_tree, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "category_tree_path": str(category_tree_path),
        "runtime_map_path": str(runtime_map_path),
        "update_count": len(updates),
        "updates": updates[:200],
    }


def main():
    parser = argparse.ArgumentParser(description="同步 category_tree 展示名为平台真实路径名称")
    parser.add_argument("--category-tree", type=Path, default=DEFAULT_TREE)
    parser.add_argument("--runtime-map", type=Path, default=DEFAULT_RUNTIME_MAP)
    args = parser.parse_args()

    payload = sync_category_tree(args.category_tree, args.runtime_map)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

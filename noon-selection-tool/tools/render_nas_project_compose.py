from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy_nas_release import load_env_file, tunnel_enabled, _is_truthy_env
from ops.nas_compose_render import render_nas_project_compose


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a Docker App-safe NAS compose file.")
    parser.add_argument("--env-file", default=".env.nas", help="Path to NAS env file.")
    parser.add_argument("--input", default="docker-compose.yml", help="Source compose file.")
    parser.add_argument("--output", default="docker-compose.yaml", help="Rendered compose output.")
    parser.add_argument(
        "--tunnel",
        choices=("auto", "enabled", "disabled"),
        default="auto",
        help="Whether to include the cloudflared service.",
    )
    args = parser.parse_args()

    env_file = (ROOT / args.env_file).resolve()
    input_path = (ROOT / args.input).resolve()
    output_path = (ROOT / args.output).resolve()

    env_values = load_env_file(env_file)
    include_tunnel = tunnel_enabled(env_values, args.tunnel)
    remote_category_node_enabled = _is_truthy_env(env_values.get("NOON_REMOTE_CATEGORY_NODE_ENABLED"))
    rendered = render_nas_project_compose(
        input_path.read_text(encoding="utf-8"),
        env_values=env_values,
        include_tunnel=include_tunnel,
        remote_category_node_enabled=remote_category_node_enabled,
    )
    output_path.write_text(rendered, encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

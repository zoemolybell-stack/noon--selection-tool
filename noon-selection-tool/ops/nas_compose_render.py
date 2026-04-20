from __future__ import annotations

import re


VAR_WITH_DEFAULT_PATTERN = re.compile(r"\$\{([A-Z0-9_]+):-([^}]*)\}")
VAR_REQUIRED_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def interpolate_compose_env(compose_text: str, env_values: dict[str, str]) -> str:
    def replace_with_default(match: re.Match[str]) -> str:
        key = match.group(1)
        default = match.group(2)
        return env_values.get(key, default)

    rendered = VAR_WITH_DEFAULT_PATTERN.sub(replace_with_default, compose_text)

    def replace_required(match: re.Match[str]) -> str:
        key = match.group(1)
        return env_values.get(key, "")

    return VAR_REQUIRED_PATTERN.sub(replace_required, rendered)

def strip_service(compose_text: str, service_name: str) -> str:
    lines = compose_text.splitlines()
    output: list[str] = []
    skip = False
    service_prefix = f"  {service_name}:"
    for line in lines:
        if not skip and line == service_prefix:
            skip = True
            continue
        if skip:
            if line.startswith("  ") and not line.startswith("    ") and line.endswith(":"):
                skip = False
                output.append(line)
                continue
            if line.startswith("networks:") or line.startswith("volumes:") or line.startswith("secrets:") or line.startswith("configs:"):
                skip = False
                output.append(line)
                continue
            continue
        output.append(line)
    return "\n".join(output) + ("\n" if compose_text.endswith("\n") else "")


def render_nas_project_compose(
    compose_text: str,
    *,
    env_values: dict[str, str],
    include_tunnel: bool,
    remote_category_node_enabled: bool,
) -> str:
    rendered = interpolate_compose_env(compose_text, env_values)
    if remote_category_node_enabled:
        rendered = strip_service(rendered, "category-worker")
    if not include_tunnel:
        rendered = strip_service(rendered, "cloudflared")
    return rendered

from __future__ import annotations

import unittest

from ops.nas_compose_render import interpolate_compose_env, render_nas_project_compose


SAMPLE_COMPOSE = """name: ${COMPOSE_PROJECT_NAME:-huihaokang-stable}

services:
  postgres:
    image: postgres:17-alpine
    volumes:
      - ${NOON_HOST_POSTGRES_DIR:-./postgres}:/var/lib/postgresql/data
  web:
    image: huihaokang-app:${HUIHAOKANG_IMAGE_TAG:-stable}
    volumes:
      - ${NOON_HOST_DATA_DIR:-./data}:/app/data
      - ${NOON_HOST_RUNTIME_DATA_DIR:-./runtime_data}:/app/runtime_data
      - ${NOON_HOST_BROWSER_PROFILES_DIR:-./browser_profiles}:/app/browser_profiles
      - ${NOON_HOST_LOGS_DIR:-./logs}:/app/logs
  category-worker:
    image: huihaokang-app:${HUIHAOKANG_IMAGE_TAG:-stable}
    profiles: ["local-category"]
    command: python run_task_scheduler.py --mode worker --worker-type category
  cloudflared:
    image: cloudflare/cloudflared:latest
    profiles: ["tunnel"]
    command: tunnel --no-autoupdate run

networks:
  stable-net:
    name: ${NOON_STABLE_NETWORK_NAME:-huihaokang-stable-net}
"""


class NasComposeRenderTests(unittest.TestCase):
    def test_interpolate_compose_env_resolves_defaults(self):
        rendered = interpolate_compose_env(
            SAMPLE_COMPOSE,
            {
                "HUIHAOKANG_IMAGE_TAG": "r67",
                "NOON_HOST_DATA_DIR": "/volume1/docker/huihaokang-erp/shared/data",
            },
        )
        self.assertIn("image: huihaokang-app:r67", rendered)
        self.assertIn("/volume1/docker/huihaokang-erp/shared/data:/app/data", rendered)
        self.assertIn("./runtime_data:/app/runtime_data", rendered)

    def test_render_nas_project_compose_strips_remote_category_and_disabled_tunnel(self):
        rendered = render_nas_project_compose(
            SAMPLE_COMPOSE,
            env_values={
                "HUIHAOKANG_IMAGE_TAG": "r67",
                "NOON_HOST_POSTGRES_DIR": "/volume1/docker/huihaokang-erp/shared/postgres",
                "NOON_HOST_DATA_DIR": "/volume1/docker/huihaokang-erp/shared/data",
                "NOON_HOST_RUNTIME_DATA_DIR": "/volume1/docker/huihaokang-erp/shared/runtime_data",
                "NOON_HOST_BROWSER_PROFILES_DIR": "/volume1/docker/huihaokang-erp/shared/browser_profiles",
                "NOON_HOST_LOGS_DIR": "/volume1/docker/huihaokang-erp/shared/logs",
            },
            include_tunnel=False,
            remote_category_node_enabled=True,
        )
        self.assertNotIn("category-worker:", rendered)
        self.assertNotIn("cloudflared:", rendered)
        self.assertIn("/volume1/docker/huihaokang-erp/shared/postgres:/var/lib/postgresql/data", rendered)
        self.assertIn("/volume1/docker/huihaokang-erp/shared/runtime_data:/app/runtime_data", rendered)
        self.assertIn("image: huihaokang-app:r67", rendered)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any


DEFAULT_V2RAYN_DB = Path(r"D:\v2rayN-windows-64\guiConfigs\guiNDB.db")


def resolve_default_v2rayn_db_path() -> Path:
    raw = str(
        Path(
            os.getenv("NOON_V2RAYN_DB_PATH")
            or os.getenv("V2RAYN_DB_PATH")
            or DEFAULT_V2RAYN_DB
        )
    ).strip()
    return Path(raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a standalone Xray config for one v2rayN profile slot."
    )
    parser.add_argument("--db", type=Path, default=resolve_default_v2rayn_db_path())
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--profile-index-id")
    selector.add_argument("--profile-remarks")
    parser.add_argument("--remarks-match", choices=["exact", "contains"], default="exact")
    parser.add_argument("--socks-port", type=int, required=True)
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _load_profile(
    db_path: Path,
    *,
    profile_index_id: str = "",
    profile_remarks: str = "",
    remarks_match: str = "exact",
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        if profile_index_id:
            row = conn.execute(
                "SELECT * FROM ProfileItem WHERE IndexId = ?",
                (profile_index_id,),
            ).fetchone()
        else:
            remarks = str(profile_remarks or "").strip()
            if remarks_match == "contains":
                row = conn.execute(
                    "SELECT * FROM ProfileItem WHERE Remarks LIKE ? ORDER BY Remarks ASC LIMIT 1",
                    (f"%{remarks}%",),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM ProfileItem WHERE Remarks = ? ORDER BY Remarks ASC LIMIT 1",
                    (remarks,),
                ).fetchone()
    finally:
        conn.close()
    if row is None:
        selector = profile_index_id or profile_remarks
        raise SystemExit(f"profile not found: {selector}")
    return dict(row)


def _parse_json_object(raw: Any) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _build_stream_settings(profile: dict[str, Any]) -> dict[str, Any]:
    network = str(profile.get("Network") or "tcp").strip() or "tcp"
    stream_security = str(profile.get("StreamSecurity") or "").strip() or "none"
    settings: dict[str, Any] = {
        "network": network,
        "security": stream_security,
    }
    request_host = str(profile.get("RequestHost") or "").strip()
    path = str(profile.get("Path") or "").strip()
    sni = str(profile.get("Sni") or "").strip()
    alpn = str(profile.get("Alpn") or "").strip()
    fingerprint = str(profile.get("Fingerprint") or "").strip()

    if sni:
        settings["tlsSettings"] = {"serverName": sni}
    if alpn:
        tls_settings = settings.setdefault("tlsSettings", {})
        tls_settings["alpn"] = [item.strip() for item in alpn.split(",") if item.strip()]
    if fingerprint:
        tls_settings = settings.setdefault("tlsSettings", {})
        tls_settings["fingerprint"] = fingerprint

    if network == "ws":
        ws_settings: dict[str, Any] = {}
        if path:
            ws_settings["path"] = path
        if request_host:
            ws_settings["headers"] = {"Host": request_host}
        settings["wsSettings"] = ws_settings
    elif network in {"h2", "http"}:
        http_settings: dict[str, Any] = {}
        if path:
            http_settings["path"] = path
        if request_host:
            http_settings["host"] = [item.strip() for item in request_host.split(",") if item.strip()]
        settings["httpSettings"] = http_settings
    elif network == "grpc":
        grpc_settings: dict[str, Any] = {}
        if path:
            grpc_settings["serviceName"] = path
        settings["grpcSettings"] = grpc_settings

    return settings


def _build_vmess_outbound(profile: dict[str, Any]) -> dict[str, Any]:
    proto_extra = _parse_json_object(profile.get("ProtoExtra"))
    security = (
        str(proto_extra.get("VmessSecurity") or "").strip()
        or str(profile.get("Security") or "").strip()
        or "auto"
    )
    alter_id_raw = proto_extra.get("AlterId") or profile.get("AlterId") or 0
    try:
        alter_id = int(alter_id_raw)
    except (TypeError, ValueError):
        alter_id = 0

    user_id = str(profile.get("Id") or "").strip() or str(profile.get("Password") or "").strip()
    if not user_id:
        raise SystemExit("vmess profile is missing uuid/id")

    address = str(profile.get("Address") or "").strip()
    if not address:
        raise SystemExit("profile is missing address")

    try:
        port = int(profile.get("Port") or 0)
    except (TypeError, ValueError):
        port = 0
    if port <= 0:
        raise SystemExit("profile is missing port")

    return {
        "tag": "proxy",
        "protocol": "vmess",
        "settings": {
            "vnext": [
                {
                    "address": address,
                    "port": port,
                    "users": [
                        {
                            "id": user_id,
                            "alterId": alter_id,
                            "security": security,
                        }
                    ],
                }
            ]
        },
        "streamSettings": _build_stream_settings(profile),
    }


def build_xray_config(profile: dict[str, Any], *, listen: str, socks_port: int) -> dict[str, Any]:
    config_type = int(profile.get("ConfigType") or 0)
    if config_type != 1:
        raise SystemExit(f"unsupported config type for slot rendering: {config_type}")

    return {
        "log": {"loglevel": "warning"},
        "inbounds": [
            {
                "tag": "socks-in",
                "port": socks_port,
                "listen": listen,
                "protocol": "socks",
                "settings": {
                    "auth": "noauth",
                    "udp": True,
                },
            }
        ],
        "outbounds": [
            _build_vmess_outbound(profile),
            {
                "tag": "direct",
                "protocol": "freedom",
            },
        ],
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    profile = _load_profile(
        args.db,
        profile_index_id=str(args.profile_index_id or "").strip(),
        profile_remarks=str(args.profile_remarks or "").strip(),
        remarks_match=str(args.remarks_match or "exact").strip().lower(),
    )
    config = build_xray_config(profile, listen=args.listen, socks_port=args.socks_port)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "profile_index_id": str(profile.get("IndexId") or "").strip(),
                "remarks": str(profile.get("Remarks") or "").strip(),
                "address": str(profile.get("Address") or "").strip(),
                "port": int(profile.get("Port") or 0),
                "socks_port": args.socks_port,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

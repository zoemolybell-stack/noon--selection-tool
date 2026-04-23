# Remote Category Multi-Slot Deployment

Last Updated: 2026-04-22

## Goal

Turn one Windows host into a reusable `2-slot` remote category node:

- one device
- two independent local proxy exits
- two independent Chrome instances
- two independent Chrome profiles
- two independent category workers
- one NAS control plane

This is the correct scaling unit for browser scraping on a single machine.

It is not:

- "switch the whole PC to another v2rayN node"
- "just add more Docker containers on one shared browser"
- "multiple workers sharing one IP and one browser identity"

## Current Contract

Each slot is:

- `proxy`
- `chrome`
- `profile`
- `cdp endpoint`
- `category worker`

Concrete shape:

- `slot-a`
  - `127.0.0.1:<slot_a_socks_port>`
  - Chrome `--remote-debugging-port=<slot_a_cdp_port>`
  - profile root `...\\slot-a`
  - worker name `remote-category-slot-a`
- `slot-b`
  - `127.0.0.1:<slot_b_socks_port>`
  - Chrome `--remote-debugging-port=<slot_b_cdp_port>`
  - profile root `...\\slot-b`
  - worker name `remote-category-slot-b`

## Prerequisites

- Windows host
- v2rayN installed locally
- v2rayN profile database available
- xray executable available
- existing `.env.category-node` already works on this machine
  - this remains the source of NAS DB / SSH tunnel / category node runtime settings

## Files

- [docker-compose.category-slots.yml](../docker-compose.category-slots.yml)
  - two category workers plus one NAS DB tunnel
- [tools/manage_remote_category_slots.ps1](../tools/manage_remote_category_slots.ps1)
  - main operator entrypoint
- [tools/test_dual_category_slots.ps1](../tools/test_dual_category_slots.ps1)
  - verification flow for two slots
- [tools/list_v2ray_profiles.py](../tools/list_v2ray_profiles.py)
  - inspect available v2rayN profiles on a host
- [tools/render_v2ray_slot_xray_config.py](../tools/render_v2ray_slot_xray_config.py)
  - builds one standalone Xray config from one v2rayN profile
- [.env.category-slots.example](../.env.category-slots.example)
  - per-device slot config template

## Setup

1. Copy `.env.category-slots.example` to `.env.category-slots`.
2. Fill:
   - `NOON_V2RAYN_DB_PATH`
   - `NOON_V2RAYN_XRAY_PATH`
   - `NOON_REMOTE_CATEGORY_SLOT_PROFILE_ROOT`
3. Select two v2rayN nodes.

To inspect available nodes:

```powershell
python tools/list_v2ray_profiles.py --filter 香港
python tools/list_v2ray_profiles.py --filter 阿联酋
```

Prefer using `SLOT_A_PROFILE_REMARKS` / `SLOT_B_PROFILE_REMARKS`.

Reason:

- remarks are more portable across devices
- `IndexId` can differ per machine or after subscription refresh

## Commands

Status:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/manage_remote_category_slots.ps1 -Action status
```

Ensure runtime:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/manage_remote_category_slots.ps1 -Action ensure
```

Stop runtime:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/manage_remote_category_slots.ps1 -Action stop
```

Verification test:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/manage_remote_category_slots.ps1 -Action test
```

`test` defaults to cleanup after verification.

If you want to keep the slot processes alive after test:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/manage_remote_category_slots.ps1 -Action test -LeaveRunning
```

## What `ensure` Actually Does

1. Render two standalone Xray configs from v2rayN profile selectors.
2. Start two local slot proxies.
3. Start two hidden Chrome instances with:
   - different CDP ports
   - different profiles
   - different proxy servers
4. Start two category workers through docker compose.
5. Verify control-plane heartbeat presence.

## Validation Standard

Healthy result means:

- slot A and slot B expose different local SOCKS ports
- slot A and slot B expose different CDP ports
- slot A and slot B resolve to independent proxy exits
- NAS `/api/workers` shows:
  - `remote-category-slot-a`
  - `remote-category-slot-b`

## Current Scope and Limitations

- current renderer supports the v2rayN `ConfigType=1` profiles already used on this machine
- this is a `single-host multi-slot` solution, not a multi-machine cluster
- the category queue is still a shared NAS queue
- multiple workers lease work from the same category task pool
- this improves browser/IP isolation substantially, but does not create full hardware isolation

## Recommended Usage

Use this for:

- scaling one remote category node on one stronger desktop
- reducing single-browser pressure
- testing multiple exit identities before moving to more hardware

Do not treat it as the final cluster architecture.

The next level after this is:

- more devices
- one or two slots per device
- same NAS control plane

# Cloudflare Tunnel and Access

## Goal

Expose `会好康ERP` on NAS stable through Cloudflare only.  
No database, worker, or scheduler port is exposed directly.

## Required Inputs

- production domain
- Cloudflare-managed DNS zone
- Tunnel token
- Access policy target users or emails

## Minimum Configuration

- NAS web service remains internal:
  - `http://web:8865`
- Cloudflare tunnel publishes only the ERP web route
- Cloudflare Access protects the route before the application is reachable

## Deployment Order

1. Finish internal NAS validation first
2. Add `TUNNEL_TOKEN` to `.env.nas`
3. Start compose with cloudflared profile
4. Verify tunnel is healthy
5. Bind domain to tunnel
6. Create Cloudflare Access application
7. Allow only approved users
8. Verify login flow and ERP reachability

## Guardrails

- Do not expose:
  - SQLite files
  - scheduler ports
  - worker ports
  - SSH or NAS file shares
- Keep Access enabled even if tunnel is healthy
- Rotate tunnel secrets if they were ever pasted into an unsafe location

## Verification

- Tunnel route resolves through Cloudflare
- Access login challenge is shown
- After login:
  - `/api/health` returns `200`
  - `/api/system/health` returns `200`
  - ERP web loads normally
- `python validate_tunnel_origin.py` confirms:
  - `huihaokang-cloudflared` and `huihaokang-web` share `huihaokang-stable-net`
  - no `lookup web ... no such host` errors remain in tunnel logs

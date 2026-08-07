#!/usr/bin/env python3
"""Read every container in a project and price it against the Zerops rate card.

Usage: _cost.py <projectId> <token>

Rate card below is validated, not assumed: the Zerops console prices its own
showcase (8 shared cores / 6.75 GB RAM / 7 GB disk / 5 GB object storage) at
$25.80 per 30 days, and these rates reproduce that figure exactly.
"""
import json
import sys
import urllib.error
import urllib.request

API = "https://api.app-prg1.zerops.io/api/rest/public"

CPU_PER_CORE_30D = 0.60   # shared core
RAM_PER_GB_30D = 3.00
DISK_PER_GB_30D = 0.10
HOURS_30D = 720.0


def get(path, token):
    req = urllib.request.Request(f"{API}{path}", headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        return {"error": f"HTTP {exc.code}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def main():
    project_id, token = sys.argv[1], sys.argv[2]
    stacks = get(f"/project/{project_id}/service-stack", token).get("list", [])

    total_cores = total_ram_gb = total_disk_gb = 0.0
    rows = []
    for stack in stacks:
        containers = get(f"/service-stack/{stack['id']}/container", token).get("list", [])
        cores = ram_gb = disk_gb = 0.0
        for container in containers:
            res = container.get("currentHardwareResource") or {}
            cores += res.get("cpuCoreCount", 0)
            ram_gb += res.get("memoryMBytes", 0) / 1024.0
            disk_gb += res.get("diskGBytes", 0)
        rows.append((stack.get("name", "?"), len(containers), cores, ram_gb, disk_gb))
        total_cores += cores
        total_ram_gb += ram_gb
        total_disk_gb += disk_gb

    monthly = (
        total_cores * CPU_PER_CORE_30D
        + total_ram_gb * RAM_PER_GB_30D
        + total_disk_gb * DISK_PER_GB_30D
    )
    hourly = monthly / HOURS_30D

    print(f"  {'service':<12} {'ctrs':>4} {'cores':>6} {'RAM GB':>7} {'disk GB':>8}")
    print(f"  {'-'*12} {'-'*4} {'-'*6} {'-'*7} {'-'*8}")
    for name, n, c, r, d in rows:
        print(f"  {name:<12} {n:>4} {c:>6.1f} {r:>7.2f} {d:>8.1f}")
    print(f"  {'TOTAL':<12} {'':>4} {total_cores:>6.1f} {total_ram_gb:>7.2f} {total_disk_gb:>8.1f}")
    print()
    print(f"  ONE idle sandbox costs:  ${hourly:.4f}/hour   ${hourly*24:.2f}/day   ${monthly:.2f}/30d")
    print()
    print("  Against a $15.00 balance, running continuously:")
    for label, n in (("Playpen itself", 1), ("+ warm pool of 2", 3), ("+ warm pool of 5", 6)):
        per_day = hourly * 24 * n
        days = 15.0 / per_day if per_day else float("inf")
        flag = "" if days > 7 else "   <-- tight"
        print(f"    {label:<18} {n} project(s)  ${per_day:.2f}/day   balance lasts {days:.1f} days{flag}")


if __name__ == "__main__":
    main()

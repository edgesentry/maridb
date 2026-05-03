"""AIS stream region rotator.

Keeps exactly --max-slots concurrent ais_stream processes running, cycling
through all regions in round-robin order.  When a slot's --slot-duration
expires the process exits cleanly and the next region in the queue starts.

Usage:
    uv run python scripts/ais_rotate.py
    uv run python scripts/ais_rotate.py --max-slots 3 --slot-duration 3600
    uv run python scripts/ais_rotate.py --regions blacksea,japansea,singapore
"""

import argparse
import asyncio
import os
import signal
import sys
from itertools import cycle
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path.home() / ".indago" / "env")

# fmt: off
REGIONS: list[tuple[str, list[float]]] = [
    ("singapore",    [-5,  92,  22, 122]),
    ("japansea",     [25, 115,  48, 145]),
    ("blacksea",     [40,  27,  48,  42]),
    ("europe",       [35, -10,  65,  30]),
    ("middleeast",   [10,  32,  32,  62]),
    ("persiangulf",  [20,  48,  30,  65]),
    ("gulfofguinea", [-5,  -5,  10,  10]),
    ("gulfofaden",   [10,  42,  16,  52]),
    ("gulfofmexico", [18, -98,  31, -80]),
    ("hornofafrica", [-5,  40,  15,  55]),
]
# fmt: on


async def _pipe(stream: asyncio.StreamReader, prefix: str, dest) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        dest.buffer.write(f"[{prefix}] ".encode() + line)
        dest.buffer.flush()


async def run_slot(
    name: str,
    bbox: list[float],
    db_path: Path,
    api_key: str,
    uv_bin: str,
    project_dir: Path,
    slot_duration: int,
    stop: asyncio.Event,
) -> None:
    lat_min, lon_min, lat_max, lon_max = bbox
    cmd = [
        uv_bin, "run", "--project", str(project_dir),
        "python", "-m", "pipelines.ingest.ais_stream",
        "--db", str(db_path),
        "--bbox", str(lat_min), str(lon_min), str(lat_max), str(lon_max),
        "--duration", str(slot_duration),
        "--flush-interval", "60",
    ]
    env = {**os.environ, "AISSTREAM_API_KEY": api_key}

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await asyncio.gather(
        _pipe(proc.stdout, name, sys.stdout),
        _pipe(proc.stderr, name, sys.stderr),
        proc.wait(),
    )

    if stop.is_set() and proc.returncode is None:
        proc.terminate()
        await proc.wait()


async def rotate(
    api_key: str,
    regions: list[tuple[str, list[float]]],
    max_slots: int,
    slot_duration: int,
    data_dir: Path,
    project_dir: Path,
    uv_bin: str,
) -> None:
    stop = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    region_cycle = cycle(regions)
    active: dict[asyncio.Task, str] = {}

    def _next_task() -> asyncio.Task:
        name, bbox = next(region_cycle)
        db_path = data_dir / f"{name}.duckdb"
        print(f"[rotator] starting {name} (slot_duration={slot_duration}s)", flush=True)
        return asyncio.create_task(
            run_slot(name, bbox, db_path, api_key, uv_bin, project_dir, slot_duration, stop),
            name=name,
        )

    for _ in range(min(max_slots, len(regions))):
        t = _next_task()
        active[t] = t.get_name()

    while active:
        done, _ = await asyncio.wait(list(active), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            del active[task]
            if not stop.is_set():
                t = _next_task()
                active[t] = t.get_name()

    print("[rotator] all slots stopped", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="AIS stream region rotator")
    parser.add_argument("--max-slots", type=int, default=3, metavar="N",
                        help="Max concurrent streams (default: 3)")
    parser.add_argument("--slot-duration", type=int, default=3600, metavar="SECS",
                        help="Seconds per region per slot (default: 3600)")
    parser.add_argument("--data-dir",
                        default=str(Path.home() / ".indago" / "data" / "raw" / "ais"),
                        metavar="DIR", help="Directory for regional .duckdb files")
    parser.add_argument("--regions", default=None, metavar="r1,r2,...",
                        help="Comma-separated subset of regions (default: all)")
    parser.add_argument("--uv-bin", default=None, metavar="PATH",
                        help="Path to uv binary (default: auto-detect)")
    args = parser.parse_args()

    api_key = os.getenv("AISSTREAM_API_KEY")
    if not api_key:
        raise SystemExit("AISSTREAM_API_KEY not set — add it to ~/.indago/env or export it")

    regions = REGIONS
    if args.regions:
        names = {r.strip() for r in args.regions.split(",")}
        regions = [(n, b) for n, b in REGIONS if n in names]
        unknown = names - {n for n, _ in regions}
        if unknown:
            raise SystemExit(f"Unknown regions: {', '.join(sorted(unknown))}\n"
                             f"Available: {', '.join(n for n, _ in REGIONS)}")

    uv_bin = args.uv_bin or _find_uv()
    project_dir = Path(__file__).resolve().parents[1]
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    print(f"[rotator] slots={args.max_slots} duration={args.slot_duration}s "
          f"regions={[n for n, _ in regions]}", flush=True)

    asyncio.run(rotate(
        api_key=api_key,
        regions=regions,
        max_slots=args.max_slots,
        slot_duration=args.slot_duration,
        data_dir=data_dir,
        project_dir=project_dir,
        uv_bin=uv_bin,
    ))


def _find_uv() -> str:
    import shutil
    return shutil.which("uv") or "/opt/homebrew/bin/uv"


if __name__ == "__main__":
    main()

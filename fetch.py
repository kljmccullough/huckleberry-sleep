"""
Huckleberry sleep fetcher.

Runs on GitHub Actions. Logs into Huckleberry via the unofficial
`huckleberry-api` library, pulls recent sleep intervals for one child,
and writes two files that the Cowork daily routine reads:

  data/latest.json     -> most recent snapshot (overwritten each run)
  data/history.jsonl   -> one JSON line appended per run (audit trail)

Design choices:
- The output is intentionally "dumb": just times, durations and the
  child's age. No names, no account info -> safe for a public repo.
  (Credentials live only in GitHub Actions secrets, never in output.)
- All interpretation (nap vs night, recommendations) is done later by
  Claude in the Cowork routine, not here.

Required environment variables (set as GitHub Actions secrets/vars):
  HUCKLEBERRY_EMAIL      your Huckleberry login email      (secret)
  HUCKLEBERRY_PASSWORD   your Huckleberry login password   (secret)

Optional environment variables (set as repo "Variables", not secrets):
  HB_TIMEZONE   IANA tz, e.g. "America/New_York". Default: account tz.
  HB_CHILD_INDEX   which child if you track more than one. Default: 0.
  HB_LOOKBACK_DAYS   how many days of history to pull. Default: 14.
  HB_INCLUDE_NICKNAME   "1" to include the child's nickname in output.
                        Default: off (keeps output anonymous).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def _parse_birthdate(raw) -> str | None:
    """Huckleberry stores birthdate as an ISO-ish string or epoch seconds."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    s = str(raw).strip()
    # Try a few common shapes; fall back to the raw first 10 chars.
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[: len(fmt) + 4], fmt).date().isoformat()
        except ValueError:
            continue
    return s[:10] or None


def _age_days(birth_iso: str | None) -> int | None:
    if not birth_iso:
        return None
    try:
        bd = datetime.strptime(birth_iso, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (datetime.now(timezone.utc).date() - bd).days


def _local_iso(epoch_seconds: float, offset_minutes: float) -> str:
    """Convert an epoch-seconds timestamp + Huckleberry offset to local ISO.

    Huckleberry stores `offset` in JavaScript getTimezoneOffset() convention:
    minutes to SUBTRACT from UTC to get local time (e.g. Pacific Daylight = 420,
    US Eastern = 300). So local = UTC - offset. We negate to build the tzinfo.
    """
    tz = timezone(timedelta(minutes=-offset_minutes))
    return datetime.fromtimestamp(float(epoch_seconds), tz=tz).isoformat()


async def run() -> dict:
    import aiohttp
    from huckleberry_api import HuckleberryAPI

    email = os.environ["HUCKLEBERRY_EMAIL"]
    password = os.environ["HUCKLEBERRY_PASSWORD"]
    tz_name = os.environ.get("HB_TIMEZONE", "").strip() or None
    child_index = _env_int("HB_CHILD_INDEX", 0)
    lookback_days = _env_int("HB_LOOKBACK_DAYS", 14)
    include_nickname = os.environ.get("HB_INCLUDE_NICKNAME", "").strip() == "1"

    async with aiohttp.ClientSession() as websession:
        api = HuckleberryAPI(
            email=email,
            password=password,
            timezone=tz_name or "UTC",
            websession=websession,
        )
        await api.authenticate()

        user = await api.get_user()
        if not user or not user.childList:
            raise RuntimeError("No children found on this Huckleberry account.")
        if child_index >= len(user.childList):
            raise RuntimeError(
                f"HB_CHILD_INDEX={child_index} but account has "
                f"{len(user.childList)} child(ren)."
            )

        child_ref = user.childList[child_index]
        child_uid = child_ref.cid
        effective_tz = tz_name or getattr(user, "latestTimezone", None) or "UTC"

        child = await api.get_child(child_uid)
        birth_iso = _parse_birthdate(getattr(child, "birthdate", None)) if child else None

        # Pull a rolling window of sleep history.
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=lookback_days)
        intervals = await api.list_sleep_intervals(child_uid, start_time=start, end_time=now)

        sleeps = []
        for iv in intervals:
            start_s = float(iv.start)
            dur_s = float(iv.duration)
            off = float(iv.offset)
            end_s = start_s + dur_s
            sleeps.append(
                {
                    "start_local": _local_iso(start_s, off),
                    "end_local": _local_iso(end_s, off),
                    "start_epoch": start_s,
                    "duration_min": round(dur_s / 60.0, 1),
                    "utc_offset_min": -off,
                }
            )
        sleeps.sort(key=lambda r: r["start_epoch"])

        # Huckleberry's own "SweetSpot" nap/bedtime predictions, if present.
        sweetspot = None
        if child is not None and getattr(child, "sweetspot", None) is not None:
            ss = child.sweetspot
            sweetspot = {
                "selected_nap_day": getattr(ss, "selectedNapDay", None),
                "sweet_spot_times": getattr(ss, "sweetSpotTimes", None),
            }

        snapshot = {
            "fetched_at_utc": now.replace(microsecond=0).isoformat(),
            "timezone": effective_tz,
            "child": {
                "birthdate": birth_iso,
                "age_days": _age_days(birth_iso),
            },
            "sleep_intervals": sleeps,
            "huckleberry_sweetspot": sweetspot,
            "lookback_days": lookback_days,
            "interval_count": len(sleeps),
        }
        if include_nickname:
            snapshot["child"]["nickname"] = getattr(child_ref, "nickname", None)

        return snapshot


def main() -> None:
    snapshot = asyncio.run(run())

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "latest.json").write_text(json.dumps(snapshot, indent=2))
    with (DATA_DIR / "history.jsonl").open("a") as fh:
        fh.write(json.dumps(snapshot) + "\n")

    print(
        f"OK: {snapshot['interval_count']} sleep intervals over "
        f"{snapshot['lookback_days']} days; tz={snapshot['timezone']}."
    )


if __name__ == "__main__":
    main()

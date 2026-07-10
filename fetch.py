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
  HB_LOOKBACK_DAYS   how many days of sleep history to pull. Default: 14.
  HB_FOOD_LOOKBACK_DAYS   how many days of solids history to scan. Default: 240.
  HB_INCLUDE_NICKNAME   "1" to include the child's nickname in output.
                        Default: off (keeps output anonymous).

Outputs written:
  data/latest.json     latest sleep snapshot (overwritten each run)
  data/history.jsonl   one sleep snapshot appended per run
  data/food.json       solids: foods tried to date + curated catalog with
                       allergen/age flags (overwritten each run)
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


def _norm(name: str) -> str:
    """Normalize a food name for matching (lowercase, trimmed)."""
    return " ".join(str(name).strip().lower().split())


def _aggregate_foods_tried(solids_intervals) -> list[dict]:
    """Roll up solids feed intervals into one row per distinct food.

    Each interval has a `foods` map (SolidsFoodEntry) and an optional
    `reactions` map. We aggregate by normalized food name, tracking how
    many times it appeared, first/last dates, and any recorded reactions.
    """
    agg: dict[str, dict] = {}
    for iv in solids_intervals:
        foods = getattr(iv, "foods", None) or {}
        reactions_map = getattr(iv, "reactions", None) or {}
        reactions = [r for r, on in reactions_map.items() if on]
        start = float(getattr(iv, "start", 0) or 0)
        off = float(getattr(iv, "offset", 0) or 0)
        when = _local_iso(start, off) if start else None
        for entry in foods.values():
            name = getattr(entry, "created_name", None)
            if not name:
                continue
            key = _norm(name)
            row = agg.get(key)
            if row is None:
                row = {
                    "name": str(name).strip(),
                    "source": getattr(entry, "source", None),
                    "times_tried": 0,
                    "first_tried_local": when,
                    "last_tried_local": when,
                    "reactions": set(),
                }
                agg[key] = row
            row["times_tried"] += 1
            if when:
                if not row["first_tried_local"] or when < row["first_tried_local"]:
                    row["first_tried_local"] = when
                if not row["last_tried_local"] or when > row["last_tried_local"]:
                    row["last_tried_local"] = when
            row["reactions"].update(reactions)
    out = []
    for row in agg.values():
        row["reactions"] = sorted(row["reactions"])
        out.append(row)
    out.sort(key=lambda r: (r["first_tried_local"] or "", r["name"].lower()))
    return out


def _shape_catalog(curated, tried_names: set[str]) -> list[dict]:
    """Slim the curated food catalog and mark which foods are already tried."""
    items = []
    for food in curated:
        name = getattr(food, "name", None)
        if not name:
            continue
        cat = getattr(food, "category", None) or {}
        categories = sorted([k for k, on in cat.items() if on])
        items.append(
            {
                "name": str(name).strip(),
                "is_common_allergen": bool(getattr(food, "is_common_allergen", False)),
                "is_high_choking_hazard": bool(getattr(food, "is_high_choking_hazard", False)),
                "recommended_age_months": getattr(food, "recommended_age_to_start", None),
                "categories": categories,
                "already_tried": _norm(name) in tried_names,
            }
        )
    return items


async def run() -> dict:
    import aiohttp
    from huckleberry_api import HuckleberryAPI

    email = os.environ["HUCKLEBERRY_EMAIL"]
    password = os.environ["HUCKLEBERRY_PASSWORD"]
    tz_name = os.environ.get("HB_TIMEZONE", "").strip() or None
    child_index = _env_int("HB_CHILD_INDEX", 0)
    lookback_days = _env_int("HB_LOOKBACK_DAYS", 14)
    food_lookback_days = _env_int("HB_FOOD_LOOKBACK_DAYS", 240)
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

        age_days = _age_days(birth_iso)
        snapshot = {
            "fetched_at_utc": now.replace(microsecond=0).isoformat(),
            "timezone": effective_tz,
            "child": {
                "birthdate": birth_iso,
                "age_days": age_days,
            },
            "sleep_intervals": sleeps,
            "huckleberry_sweetspot": sweetspot,
            "lookback_days": lookback_days,
            "interval_count": len(sleeps),
        }
        if include_nickname:
            snapshot["child"]["nickname"] = getattr(child_ref, "nickname", None)

        # ---- Solids / food ----
        food_start = now - timedelta(days=food_lookback_days)
        feed_intervals = await api.list_feed_intervals(
            child_uid, start_time=food_start, end_time=now
        )
        solids = [iv for iv in feed_intervals if getattr(iv, "mode", None) == "solids"]
        foods_tried = _aggregate_foods_tried(solids)
        tried_names = {_norm(r["name"]) for r in foods_tried}

        try:
            curated = await api.list_solids_curated_foods()
        except Exception as err:  # noqa: BLE001 - catalog is best-effort
            print(f"WARN: could not load curated food catalog: {err}")
            curated = []
        catalog = _shape_catalog(curated, tried_names)

        food_snapshot = {
            "fetched_at_utc": now.replace(microsecond=0).isoformat(),
            "timezone": effective_tz,
            "child": {"birthdate": birth_iso, "age_days": age_days},
            "foods_tried": foods_tried,
            "foods_tried_count": len(foods_tried),
            "food_catalog": catalog,
            "food_lookback_days": food_lookback_days,
        }

        return {"sleep": snapshot, "food": food_snapshot}


def main() -> None:
    result = asyncio.run(run())
    snapshot = result["sleep"]
    food_snapshot = result["food"]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "latest.json").write_text(json.dumps(snapshot, indent=2))
    with (DATA_DIR / "history.jsonl").open("a") as fh:
        fh.write(json.dumps(snapshot) + "\n")
    (DATA_DIR / "food.json").write_text(json.dumps(food_snapshot, indent=2))

    print(
        f"OK: {snapshot['interval_count']} sleep intervals over "
        f"{snapshot['lookback_days']} days; "
        f"{food_snapshot['foods_tried_count']} foods tried; "
        f"catalog {len(food_snapshot['food_catalog'])}; tz={snapshot['timezone']}."
    )


if __name__ == "__main__":
    main()

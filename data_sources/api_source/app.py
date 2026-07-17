"""Source 2: enrollment events REST API (cursor pagination, ~5% late events).
member_id drawn from the SHARED pool so gold joins match.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from fastapi import FastAPI, Query

from shared_ids import MEMBER_IDS

app = FastAPI(title="Enrollment Events API", version="1.0.0")

EVENT_TYPES = ["ENROLLED", "PLAN_CHANGE", "DEPENDENT_ADDED", "TERMINATED", "COBRA_ELECTED"]
PLANS = ["PPO_GOLD", "PPO_SILVER", "HMO_STANDARD", "HDHP_HSA"]

_rng = random.Random(42)
_EVENTS: list[dict] = []


def _build_events(days: int = 30, per_hour: int = 40) -> None:
    start = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=days)
    seq = 0
    for h in range(days * 24):
        created = start + timedelta(hours=h)
        for _ in range(_rng.randint(per_hour // 2, per_hour)):
            seq += 1
            if _rng.random() < 0.05:
                event_time = created - timedelta(hours=_rng.randint(1, 6))
            else:
                event_time = created + timedelta(minutes=_rng.randint(0, 59))
            _EVENTS.append({
                "event_id": f"EV{seq:010d}",
                "member_id": _rng.choice(MEMBER_IDS),
                "event_type": _rng.choice(EVENT_TYPES),
                "plan_code": _rng.choice(PLANS),
                "event_time": event_time.isoformat(),
                "created_at": created.isoformat(),
            })
    _EVENTS.sort(key=lambda e: e["created_at"])


_build_events()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "total_events": len(_EVENTS)}


@app.get("/api/v1/enrollment-events")
def get_events(
    since: str = Query(...),
    limit: int = Query(100, ge=1, le=1000),
    cursor: int = Query(0, ge=0),
) -> dict:
    since_dt = datetime.fromisoformat(since)
    matched = [e for e in _EVENTS if datetime.fromisoformat(e["event_time"]) >= since_dt]
    page = matched[cursor:cursor + limit]
    next_cursor = cursor + limit if cursor + limit < len(matched) else None
    return {"data": page, "count": len(page), "total_matched": len(matched), "next_cursor": next_cursor}

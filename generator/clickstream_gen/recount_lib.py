"""Pure-Python batch recount engine mirroring Flink SQL deduplication, temporal joins, and windows."""

from __future__ import annotations

import bisect
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable


def _round2(val: float) -> float:
    return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class WindowAggregate:
    window_start_ms: int
    window_end_ms: int
    total_events: int
    page_views: int
    add_to_carts: int
    checkouts: int
    purchases: int
    active_users: int
    gross_revenue: float
    avg_order_value: float


@dataclass(frozen=True)
class BatchRecountResult:
    raw_event_count: int
    unique_event_ids: set[str]
    duplicate_count: int
    per_user_events: dict[str, int]
    per_user_purchase_units: dict[str, int]
    tumbling_1m: dict[int, WindowAggregate]
    max_event_time_ms: int


class TemporalCatalogIndex:
    """Point-in-time lookup structure matching `FOR SYSTEM_TIME AS OF c.event_ts`."""

    def __init__(self, catalog_records: Iterable[dict[str, Any]]) -> None:
        by_product: dict[str, list[tuple[int, int, dict[str, Any]]]] = defaultdict(list)
        for seq, rec in enumerate(catalog_records):
            pid = str(rec["product_id"])
            ts = int(rec["updated_at_ms"])
            by_product[pid].append((ts, seq, rec))

        self._timestamps: dict[str, list[int]] = {}
        self._records: dict[str, list[dict[str, Any]]] = {}
        for pid, entries in by_product.items():
            # Sort by (updated_at_ms ASC, arrival seq ASC) so latest update at equal timestamp wins
            entries.sort(key=lambda item: (item[0], item[1]))
            dedup_ts: list[int] = []
            dedup_rec: list[dict[str, Any]] = []
            for ts, _, rec in entries:
                if dedup_ts and dedup_ts[-1] == ts:
                    dedup_rec[-1] = rec
                else:
                    dedup_ts.append(ts)
                    dedup_rec.append(rec)
            self._timestamps[pid] = dedup_ts
            self._records[pid] = dedup_rec

    def lookup_price(self, product_id: str, event_time_ms: int) -> float:
        ts_list = self._timestamps.get(product_id)
        if not ts_list:
            return 0.0
        idx = bisect.bisect_right(ts_list, int(event_time_ms)) - 1
        if idx < 0:
            return 0.0
        return float(self._records[product_id][idx]["price"])


def compute_batch_recount(
    raw_events: Iterable[dict[str, Any]],
    catalog_records: Iterable[dict[str, Any]],
) -> BatchRecountResult:
    """Computes exact deduplicated event totals, per-user state, and 1-minute temporal-joined windows."""
    catalog = TemporalCatalogIndex(catalog_records)

    raw_count = 0
    seen_ids: set[str] = set()
    dup_count = 0
    max_event_time_ms = 0

    per_user_events: dict[str, int] = defaultdict(int)
    per_user_units: dict[str, int] = defaultdict(int)

    win_events: dict[int, list[tuple[dict[str, Any], float]]] = defaultdict(list)

    for ev in raw_events:
        raw_count += 1
        eid = str(ev["event_id"])
        if eid in seen_ids:
            dup_count += 1
            continue
        seen_ids.add(eid)

        uid = str(ev["user_id"])
        etype = str(ev["event_type"])
        qty = int(ev.get("quantity", 1))
        event_time_ms = int(ev["event_time_ms"])
        if event_time_ms > max_event_time_ms:
            max_event_time_ms = event_time_ms

        per_user_events[uid] += 1
        if etype == "purchase":
            per_user_units[uid] += qty

        unit_price = catalog.lookup_price(str(ev["product_id"]), event_time_ms)
        line_rev = _round2(qty * unit_price) if etype == "purchase" else 0.0
        win_start = (event_time_ms // 60_000) * 60_000
        win_events[win_start].append((ev, line_rev))

    tumbling: dict[int, WindowAggregate] = {}
    for win_start in sorted(win_events.keys()):
        rows = win_events[win_start]
        total_ev = len(rows)
        views = sum(1 for r, _ in rows if r["event_type"] == "page_view")
        carts = sum(1 for r, _ in rows if r["event_type"] == "add_to_cart")
        checkouts = sum(1 for r, _ in rows if r["event_type"] == "checkout")
        purchases = sum(1 for r, _ in rows if r["event_type"] == "purchase")
        users = len({str(r["user_id"]) for r, _ in rows})
        gross = _round2(sum(rev for _, rev in rows))
        aov = _round2(gross / purchases) if purchases > 0 else 0.0
        tumbling[win_start] = WindowAggregate(
            window_start_ms=win_start,
            window_end_ms=win_start + 60_000,
            total_events=total_ev,
            page_views=views,
            add_to_carts=carts,
            checkouts=checkouts,
            purchases=purchases,
            active_users=users,
            gross_revenue=gross,
            avg_order_value=aov,
        )

    return BatchRecountResult(
        raw_event_count=raw_count,
        unique_event_ids=seen_ids,
        duplicate_count=dup_count,
        per_user_events=dict(per_user_events),
        per_user_purchase_units=dict(per_user_units),
        tumbling_1m=tumbling,
        max_event_time_ms=max_event_time_ms,
    )

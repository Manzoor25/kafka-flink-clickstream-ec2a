"""Stateful e-commerce clickstream session simulator with duplicates, jitter, late events, and spikes."""

from __future__ import annotations

import collections
import random
import uuid
from dataclasses import dataclass, field
from typing import Any

from generator.clickstream_gen.catalog import DEFAULT_PRODUCTS

DEVICES: tuple[tuple[str, float], ...] = (
    ("mobile", 0.52),
    ("desktop", 0.38),
    ("tablet", 0.10),
)

COUNTRIES: tuple[tuple[str, float], ...] = (
    ("US", 0.42),
    ("GB", 0.14),
    ("DE", 0.11),
    ("CA", 0.09),
    ("IN", 0.08),
    ("FR", 0.06),
    ("AU", 0.05),
    ("JP", 0.05),
)

REFERRERS: tuple[tuple[str, float], ...] = (
    ("google_search", 0.36),
    ("direct", 0.27),
    ("social_campaign", 0.16),
    ("email_newsletter", 0.12),
    ("affiliate_blog", 0.09),
)


def _weighted_choice(rng: random.Random, choices: tuple[tuple[str, float], ...]) -> str:
    labels = [c[0] for c in choices]
    weights = [c[1] for c in choices]
    return rng.choices(labels, weights=weights, k=1)[0]


@dataclass
class ActiveSession:
    user_id: str
    client_session_id: str
    device_type: str
    country: str
    referrer: str
    focused_product_id: str
    planned_steps: list[str]
    step_idx: int = 0


@dataclass
class GeneratorStats:
    total_emitted: int = 0
    unique_emitted: int = 0
    duplicates_emitted: int = 0
    within_watermark_ooo_emitted: int = 0
    late_events_emitted: int = 0
    by_event_type: dict[str, int] = field(
        default_factory=lambda: {"page_view": 0, "add_to_cart": 0, "checkout": 0, "purchase": 0}
    )


class ClickstreamSimulator:
    """Produces realistic multi-step user sessions plus controlled stream anomalies."""

    def __init__(
        self,
        user_pool_size: int = 600,
        duplicate_rate: float = 0.03,
        within_watermark_jitter_rate: float = 0.06,
        late_rate: float = 0.02,
        late_min_ms: int = 22_000,
        late_max_ms: int = 45_000,
        seed: int | None = None,
    ) -> None:
        self.rng = random.Random(seed)
        self.users = [f"user-{i:04d}" for i in range(1, user_pool_size + 1)]
        self.product_ids = [p.product_id for p in DEFAULT_PRODUCTS]
        # Pareto popularity weights so Top-N products shows realistic skew
        self.product_weights = [1.0 / ((idx + 1) ** 0.72) for idx in range(len(self.product_ids))]

        self.duplicate_rate = duplicate_rate
        self.within_watermark_jitter_rate = within_watermark_jitter_rate
        self.late_rate = late_rate
        self.late_min_ms = late_min_ms
        self.late_max_ms = late_max_ms

        self._active_sessions: dict[str, ActiveSession] = {}
        self._recent_events: collections.deque[dict[str, Any]] = collections.deque(maxlen=512)
        self.stats = GeneratorStats()

    def _plan_funnel_steps(self) -> list[str]:
        views = self.rng.randint(1, 3)
        steps = ["page_view"] * views
        if self.rng.random() < 0.44:
            steps.append("add_to_cart")
            if self.rng.random() < 0.62:
                steps.append("checkout")
                if self.rng.random() < 0.70:
                    steps.append("purchase")
        return steps

    def _next_session_step(self) -> tuple[ActiveSession, str]:
        # Continue an active user session ~70% of the time so multi-step funnels complete naturally
        if self._active_sessions and self.rng.random() < 0.70:
            user_id = self.rng.choice(list(self._active_sessions.keys()))
            sess = self._active_sessions[user_id]
        else:
            user_id = self.rng.choice(self.users)
            sess = self._active_sessions.get(user_id)
            if sess is None:
                sess = ActiveSession(
                    user_id=user_id,
                    client_session_id=f"csess-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12]}",
                    device_type=_weighted_choice(self.rng, DEVICES),
                    country=_weighted_choice(self.rng, COUNTRIES),
                    referrer=_weighted_choice(self.rng, REFERRERS),
                    focused_product_id=self.rng.choices(self.product_ids, weights=self.product_weights, k=1)[0],
                    planned_steps=self._plan_funnel_steps(),
                )
                self._active_sessions[user_id] = sess

        step = sess.planned_steps[sess.step_idx]
        sess.step_idx += 1
        if sess.step_idx >= len(sess.planned_steps):
            self._active_sessions.pop(sess.user_id, None)
        return sess, step

    def next_event(self, now_ms: int) -> dict[str, Any]:
        """Generates the next clickstream event (fresh, duplicate retry, jittered, or late)."""
        # 1. Controlled duplicate retry (identical event_id & payload)
        if self._recent_events and self.rng.random() < self.duplicate_rate:
            original = dict(self.rng.choice(self._recent_events))
            self.stats.total_emitted += 1
            self.stats.duplicates_emitted += 1
            return original

        sess, event_type = self._next_session_step()
        # Early browsing views may inspect sibling SKUs; cart/checkout/purchase lock onto focused SKU
        if event_type == "page_view" and sess.step_idx == 1 and self.rng.random() < 0.35:
            product_id = self.rng.choices(self.product_ids, weights=self.product_weights, k=1)[0]
        else:
            product_id = sess.focused_product_id

        quantity = self.rng.choices([1, 2, 3], weights=[0.78, 0.17, 0.05], k=1)[0] if event_type != "page_view" else 1

        # 2. Event-time assignment: on-time, within-5s-watermark jitter, or >15s late event
        roll = self.rng.random()
        if roll < self.late_rate:
            lag_ms = self.rng.randint(self.late_min_ms, self.late_max_ms)
            event_time_ms = now_ms - lag_ms
            self.stats.late_events_emitted += 1
        elif roll < self.late_rate + self.within_watermark_jitter_rate:
            jitter_ms = self.rng.randint(500, 4_200)
            event_time_ms = now_ms - jitter_ms
            self.stats.within_watermark_ooo_emitted += 1
        else:
            event_time_ms = now_ms

        page_map = {
            "page_view": f"/products/{product_id}",
            "add_to_cart": f"/cart/add/{product_id}",
            "checkout": "/checkout",
            "purchase": "/checkout/complete",
        }

        event = {
            "event_id": f"evt-{uuid.UUID(int=self.rng.getrandbits(128)).hex}",
            "user_id": sess.user_id,
            "client_session_id": sess.client_session_id,
            "event_type": event_type,
            "product_id": product_id,
            "quantity": int(quantity),
            "page_url": page_map[event_type],
            "referrer": sess.referrer,
            "device_type": sess.device_type,
            "country": sess.country,
            "event_time_ms": int(event_time_ms),
            "created_at_ms": int(now_ms),
        }

        self._recent_events.append(event)
        self.stats.total_emitted += 1
        self.stats.unique_emitted += 1
        self.stats.by_event_type[event_type] += 1
        return event

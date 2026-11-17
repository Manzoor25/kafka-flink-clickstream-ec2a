"""Unit tests for Confluent-Avro wire framing, session simulator, temporal price joins, and batch recount."""

from __future__ import annotations

from pathlib import Path

from generator.clickstream_gen.avro_wire import ConfluentAvroCodec
from generator.clickstream_gen.recount_lib import TemporalCatalogIndex, compute_batch_recount
from generator.clickstream_gen.session_simulator import ClickstreamSimulator

SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "schemas"


def test_confluent_avro_wire_roundtrip_clickstream_and_catalog() -> None:
    click_codec = ConfluentAvroCodec.from_schema_file(SCHEMAS_DIR / "clickstream_event.avsc", schema_id=7)
    sample_click = {
        "event_id": "evt-001",
        "user_id": "user-0042",
        "client_session_id": "csess-abc",
        "event_type": "purchase",
        "product_id": "prod-001",
        "quantity": 2,
        "page_url": "/checkout/complete",
        "referrer": "google_search",
        "device_type": "mobile",
        "country": "US",
        "event_time_ms": 1762000012345,
        "created_at_ms": 1762000012350,
    }
    encoded = click_codec.encode(sample_click)
    assert encoded[0] == 0x00
    assert int.from_bytes(encoded[1:5], "big") == 7
    schema_id, decoded = click_codec.decode(encoded)
    assert schema_id == 7
    assert decoded == sample_click

    cat_codec = ConfluentAvroCodec.from_schema_file(SCHEMAS_DIR / "product_catalog.avsc", schema_id=3)
    sample_cat = {
        "product_id": "prod-001",
        "product_name": "AeroPro Headphones",
        "category": "Audio",
        "price": 199.20,
        "currency": "USD",
        "updated_at_ms": 1762000000000,
    }
    cat_bytes = cat_codec.encode(sample_cat)
    cat_id, cat_decoded = cat_codec.decode(cat_bytes)
    assert cat_id == 3
    assert cat_decoded["product_id"] == "prod-001"
    assert abs(cat_decoded["price"] - 199.20) < 1e-6


def test_session_simulator_funnel_causality_duplicates_and_lateness() -> None:
    sim = ClickstreamSimulator(
        user_pool_size=40,
        duplicate_rate=0.10,
        within_watermark_jitter_rate=0.10,
        late_rate=0.05,
        seed=12345,
    )
    base_ms = 1_762_000_000_000
    events = [sim.next_event(base_ms + i * 50) for i in range(1500)]

    assert len(events) == 1500
    assert sim.stats.duplicates_emitted > 50
    assert sim.stats.late_events_emitted > 20
    assert sim.stats.within_watermark_ooo_emitted > 40
    assert sim.stats.by_event_type["page_view"] > sim.stats.by_event_type["add_to_cart"]
    assert sim.stats.by_event_type["add_to_cart"] >= sim.stats.by_event_type["checkout"]
    assert sim.stats.by_event_type["checkout"] >= sim.stats.by_event_type["purchase"] > 0

    # Verify late events actually carry event_time_ms >= 22s behind created_at_ms
    late_lags = [e["created_at_ms"] - e["event_time_ms"] for e in events if e["created_at_ms"] - e["event_time_ms"] >= 20_000]
    assert len(late_lags) >= sim.stats.late_events_emitted


def test_temporal_catalog_and_batch_recount_accuracy() -> None:
    catalog = [
        {"product_id": "prod-001", "product_name": "Headphones", "category": "Audio", "price": 100.00, "currency": "USD", "updated_at_ms": 1_000},
        {"product_id": "prod-001", "product_name": "Headphones", "category": "Audio", "price": 80.00, "currency": "USD", "updated_at_ms": 30_000},
    ]
    idx = TemporalCatalogIndex(catalog)
    assert idx.lookup_price("prod-001", 500) == 0.0
    assert idx.lookup_price("prod-001", 10_000) == 100.00
    assert idx.lookup_price("prod-001", 30_000) == 80.00
    assert idx.lookup_price("prod-001", 45_000) == 80.00

    clicks = [
        # Pre-discount purchase at t=10s -> 2 * $100 = $200
        {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "product_id": "prod-001", "quantity": 2, "event_time_ms": 10_000},
        # Duplicate of e1 -> must be ignored!
        {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "product_id": "prod-001", "quantity": 2, "event_time_ms": 10_000},
        # Post-discount purchase at t=35s -> 1 * $80 = $80
        {"event_id": "e2", "user_id": "u2", "event_type": "purchase", "product_id": "prod-001", "quantity": 1, "event_time_ms": 35_000},
        # Page view at t=40s -> $0
        {"event_id": "e3", "user_id": "u2", "event_type": "page_view", "product_id": "prod-001", "quantity": 1, "event_time_ms": 40_000},
    ]
    res = compute_batch_recount(clicks, catalog)
    assert res.raw_event_count == 4
    assert res.duplicate_count == 1
    assert res.unique_event_ids == {"e1", "e2", "e3"}
    assert res.per_user_purchase_units == {"u1": 2, "u2": 1}

    win0 = res.tumbling_1m[0]
    assert win0.total_events == 3
    assert win0.purchases == 2
    assert win0.active_users == 2
    assert win0.gross_revenue == 280.00
    assert win0.avg_order_value == 140.00

"""Compacted product catalogue seed data and mid-stream temporal price mutations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from generator.clickstream_gen.avro_wire import ConfluentAvroCodec

if TYPE_CHECKING:
    from confluent_kafka import Producer


@dataclass(frozen=True)
class ProductSpec:
    product_id: str
    product_name: str
    category: str
    base_price: float
    currency: str = "USD"


DEFAULT_PRODUCTS: tuple[ProductSpec, ...] = (
    ProductSpec("prod-001", "AeroPro Noise-Cancelling Headphones", "Audio", 249.00),
    ProductSpec("prod-002", "UltraWide 34-inch Curved Monitor", "Electronics", 599.00),
    ProductSpec("prod-003", "Mechanical Tactile Keyboard TKL", "Accessories", 139.00),
    ProductSpec("prod-004", "ErgoLift Standing Desk Frame", "Home Office", 429.00),
    ProductSpec("prod-005", "PulseTrack Titanium Smartwatch", "Wearables", 299.00),
    ProductSpec("prod-006", "Thunderbolt 4 NVMe Dock", "Accessories", 219.00),
    ProductSpec("prod-007", "ProStream 4K Low-Light Webcam", "Home Office", 159.00),
    ProductSpec("prod-008", "StudioCondenser USB-C Microphone", "Audio", 129.00),
    ProductSpec("prod-009", "Vortex Wireless Gaming Mouse", "Gaming", 89.00),
    ProductSpec("prod-010", "OLED Portable Gaming Handheld", "Gaming", 549.00),
    ProductSpec("prod-011", "LumbarMesh Ergonomic Task Chair", "Home Office", 489.00),
    ProductSpec("prod-012", "GaN 140W Multi-Port Travel Charger", "Accessories", 79.00),
    ProductSpec("prod-013", "PocketANC True Wireless Earbuds", "Audio", 169.00),
    ProductSpec("prod-014", "MagSnap Portable SSD 2TB", "Electronics", 189.00),
    ProductSpec("prod-015", "SmartRing Sleep & Recovery Tracker", "Wearables", 319.00),
    ProductSpec("prod-016", "DualArm Gas-Spring Monitor Mount", "Home Office", 109.00),
    ProductSpec("prod-017", "CreatorPad Mini Macro Controller", "Accessories", 99.00),
    ProductSpec("prod-018", "SpatialBar Dolby Desktop Soundbar", "Audio", 279.00),
    ProductSpec("prod-019", "Wi-Fi 7 Tri-Band Mesh Router", "Electronics", 349.00),
    ProductSpec("prod-020", "ForceFeedback Racing Wheel Set", "Gaming", 379.00),
    ProductSpec("prod-021", "E-Ink 10-inch Note Tablet", "Electronics", 399.00),
    ProductSpec("prod-022", "DeskMat Merino Felt XL", "Accessories", 49.00),
    ProductSpec("prod-023", "SmartScale Body Composition Wi-Fi", "Wearables", 95.00),
    ProductSpec("prod-024", "KeyLight Bi-Color LED Panel", "Home Office", 145.00),
)


def build_catalog_record(product: ProductSpec, price: float, updated_at_ms: int) -> dict[str, Any]:
    return {
        "product_id": product.product_id,
        "product_name": product.product_name,
        "category": product.category,
        "price": round(float(price), 2),
        "currency": product.currency,
        "updated_at_ms": int(updated_at_ms),
    }


def publish_catalog_snapshot(
    producer: Producer,
    codec: ConfluentAvroCodec,
    updated_at_ms: int,
    price_overrides: dict[str, float] | None = None,
    topic: str = "product.catalog.v1",
) -> list[dict[str, Any]]:
    """Publishes full product catalogue keyed by raw UTF-8 product_id for Flink upsert-kafka."""
    overrides = price_overrides or {}
    emitted: list[dict[str, Any]] = []
    for spec in DEFAULT_PRODUCTS:
        price = overrides.get(spec.product_id, spec.base_price)
        rec = build_catalog_record(spec, price=price, updated_at_ms=updated_at_ms)
        producer.produce(
            topic=topic,
            key=spec.product_id.encode("utf-8"),
            value=codec.encode(rec),
        )
        emitted.append(rec)
    producer.flush(15.0)
    return emitted


def publish_price_updates(
    producer: Producer,
    codec: ConfluentAvroCodec,
    discount_multiplier: float,
    product_ids: list[str],
    updated_at_ms: int,
    topic: str = "product.catalog.v1",
) -> list[dict[str, Any]]:
    """Publishes point-in-time price changes for selected SKUs to exercise Flink temporal joins."""
    by_id = {p.product_id: p for p in DEFAULT_PRODUCTS}
    emitted: list[dict[str, Any]] = []
    for pid in product_ids:
        spec = by_id[pid]
        new_price = round(spec.base_price * discount_multiplier, 2)
        rec = build_catalog_record(spec, price=new_price, updated_at_ms=updated_at_ms)
        producer.produce(
            topic=topic,
            key=spec.product_id.encode("utf-8"),
            value=codec.encode(rec),
        )
        emitted.append(rec)
    producer.flush(10.0)
    return emitted

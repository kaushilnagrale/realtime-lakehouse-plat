"""
Sample data producer for testing the lakehouse pipeline.
Generates realistic e-commerce events and sends them to Kafka.
"""

import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from typing import Generator, Dict
from kafka import KafkaProducer
from src.config.settings import settings


# ─── Data Generation Constants ───────────────────────────────────────────

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "checkout", "purchase", "search"]
EVENT_WEIGHTS = [0.45, 0.20, 0.05, 0.10, 0.08, 0.12]  # Realistic funnel distribution

CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "beauty", "food", "toys"]

PRODUCTS = {
    "electronics": [("PROD-E01", "Wireless Headphones", 79.99), ("PROD-E02", "Smart Watch", 249.99),
                    ("PROD-E03", "USB-C Hub", 39.99), ("PROD-E04", "Bluetooth Speaker", 59.99)],
    "clothing": [("PROD-C01", "Denim Jacket", 89.99), ("PROD-C02", "Running Shoes", 129.99),
                 ("PROD-C03", "Cotton T-Shirt", 24.99), ("PROD-C04", "Wool Sweater", 69.99)],
    "home": [("PROD-H01", "Coffee Maker", 149.99), ("PROD-H02", "LED Desk Lamp", 44.99),
             ("PROD-H03", "Air Purifier", 199.99), ("PROD-H04", "Smart Thermostat", 179.99)],
    "sports": [("PROD-S01", "Yoga Mat", 29.99), ("PROD-S02", "Resistance Bands", 19.99),
               ("PROD-S03", "Water Bottle", 14.99), ("PROD-S04", "Fitness Tracker", 99.99)],
}

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]

BROWSERS = ["chrome", "safari", "firefox", "edge"]
OS_LIST = ["iOS", "Android", "Windows", "macOS", "Linux"]

COUNTRIES = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "KR"]
COUNTRY_WEIGHTS = [0.35, 0.15, 0.10, 0.08, 0.07, 0.06, 0.05, 0.05, 0.05, 0.04]

UTM_SOURCES = ["google", "facebook", "twitter", "email", "direct", "instagram", "tiktok"]
UTM_MEDIUMS = ["cpc", "organic", "social", "email", "referral", "paid", "none"]
UTM_CAMPAIGNS = ["summer_sale", "new_arrivals", "flash_deal", "loyalty_program", None, None, None]


def generate_event(
    user_pool_size: int = 500,
    session_pool_size: int = 1000,
    timestamp: datetime = None,
) -> Dict:
    """Generate a single realistic e-commerce event."""
    
    event_type = random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]
    category = random.choice(CATEGORIES)
    
    product = None
    if category in PRODUCTS:
        product = random.choice(PRODUCTS[category])
    
    ts = timestamp or datetime.utcnow() - timedelta(seconds=random.randint(0, 3600))
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": f"user_{random.randint(1, user_pool_size):05d}",
        "session_id": f"sess_{random.randint(1, session_pool_size):06d}",
        "timestamp": ts.isoformat() + "Z",
        "product_id": product[0] if product else None,
        "product_name": product[1] if product else None,
        "category": category,
        "price": product[2] if product else None,
        "quantity": random.randint(1, 5) if event_type == "purchase" else 1,
        "currency": "USD",
        "device_type": random.choices(DEVICES, DEVICE_WEIGHTS)[0],
        "browser": random.choice(BROWSERS),
        "os": random.choice(OS_LIST),
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "country": random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
        "city": random.choice(["New York", "London", "Toronto", "Berlin", "Tokyo", "Sydney"]),
        "referrer": random.choice(["google.com", "facebook.com", "direct", None]),
        "utm_source": random.choice(UTM_SOURCES),
        "utm_medium": random.choice(UTM_MEDIUMS),
        "utm_campaign": random.choice(UTM_CAMPAIGNS),
        "properties": {"page_url": f"/product/{product[0]}" if product else "/home"},
    }
    
    return event


def generate_events(count: int, **kwargs) -> Generator[Dict, None, None]:
    """Generate a stream of events."""
    for _ in range(count):
        yield generate_event(**kwargs)


def produce_to_kafka(
    events: int = 1000,
    topic: str = None,
    batch_size: int = 100,
) -> int:
    """Produce events to Kafka topic."""
    topic = topic or settings.kafka.raw_events_topic
    
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )
    
    sent = 0
    for event in generate_events(events):
        producer.send(
            topic,
            key=event["user_id"],
            value=event,
        )
        sent += 1
        
        if sent % batch_size == 0:
            producer.flush()
            print(f"  Produced {sent}/{events} events...")
    
    producer.flush()
    producer.close()
    
    print(f"✅ Produced {sent} events to topic '{topic}'")
    return sent


def save_to_json(events: int = 1000, output_path: str = "data/sample_events.json") -> str:
    """Save generated events to a JSON file (for testing without Kafka)."""
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    data = list(generate_events(events))
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Saved {len(data)} events to {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample e-commerce events")
    parser.add_argument("--events", type=int, default=1000, help="Number of events")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic")
    parser.add_argument("--mode", choices=["kafka", "json"], default="json", help="Output mode")
    parser.add_argument("--output", type=str, default="data/sample_events.json", help="JSON output path")
    
    args = parser.parse_args()
    
    if args.mode == "kafka":
        produce_to_kafka(args.events, args.topic)
    else:
        save_to_json(args.events, args.output)

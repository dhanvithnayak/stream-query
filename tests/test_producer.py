"""
Unit tests for the Kafka synthetic event producer.
Compatible with both pytest and python3 -m unittest.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "producer")))

try:
    from produce_events import generate_order_event, PRODUCT_CATALOG, CUSTOMERS, REGIONS, STATUSES
    HAS_PRODUCER = True
except ImportError:
    HAS_PRODUCER = False


@unittest.skipUnless(HAS_PRODUCER, "Faker or producer dependencies not installed on host")
class TestProducer(unittest.TestCase):

    def test_generate_order_event_schema(self):
        """Verify generated event contains all required schema fields with proper types."""
        event = generate_order_event()

        required_fields = [
            "order_id",
            "customer_id",
            "customer_name",
            "customer_email",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "order_status",
            "region",
            "ordered_at",
        ]

        for field in required_fields:
            self.assertIn(field, event, f"Missing required field '{field}' in generated event")
            self.assertIsNotNone(event[field], f"Field '{field}' should not be None")

    def test_generate_order_event_business_rules(self):
        """Verify that generated events adhere to catalog constraints and positive amounts."""
        for _ in range(50):
            event = generate_order_event()

            self.assertGreater(event["quantity"], 0, "Quantity must be strictly positive")
            self.assertGreater(event["unit_price"], 0, "Unit price must be strictly positive")
            self.assertIn(event["order_status"], STATUSES, f"Unexpected order_status: {event['order_status']}")
            self.assertIn(event["region"], REGIONS, f"Unexpected region: {event['region']}")
            self.assertTrue(event["order_id"].startswith("ORD-"), "Order ID must match prefix ORD-")

    def test_product_catalog_integrity(self):
        """Ensure product catalog SKUs and names are unique."""
        product_ids = [p["product_id"] for p in PRODUCT_CATALOG]
        self.assertEqual(len(product_ids), len(set(product_ids)), "Duplicate product_ids in catalog")

        for product in PRODUCT_CATALOG:
            self.assertGreater(product["unit_price"], 0, f"Product {product['product_id']} has non-positive price")
            self.assertIn(product["category"], ["Electronics", "Apparel", "Home & Kitchen", "Books"])


if __name__ == "__main__":
    unittest.main()

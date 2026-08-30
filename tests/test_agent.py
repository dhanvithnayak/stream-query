"""
Unit tests for the Natural Language Text-to-SQL query agent.
Compatible with both pytest and python3 -m unittest.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "agent")))

from schema_context import get_schema_context_prompt, MART_SCHEMA_DDL, BUSINESS_RULES
from llm_client import TextToSQLClient


class TestAgent(unittest.TestCase):

    def setUp(self):
        self.agent_client = TextToSQLClient(provider="offline-rule-engine")

    def test_schema_context_prompt_contains_marts(self):
        """Verify that schema context includes all core dimensional mart tables."""
        prompt = get_schema_context_prompt()

        self.assertIn("analytics.dim_customers", prompt)
        self.assertIn("analytics.dim_products", prompt)
        self.assertIn("analytics.fct_orders", prompt)
        self.assertIn("is_completed", prompt)
        self.assertIn("lifetime_value", prompt)

    def test_offline_engine_category_query(self):
        """Test SQL generation for product category sales questions."""
        sql, provider = self.agent_client.generate_sql("What are the total sales by product category?")
        self.assertEqual(provider, "offline-rule-engine")
        self.assertIn("SELECT", sql.upper())
        self.assertIn("analytics.fct_orders", sql)
        self.assertIn("analytics.dim_products", sql)
        self.assertIn("category", sql.lower())

    def test_offline_engine_top_customers_query(self):
        """Test SQL generation for top spending customers."""
        sql, provider = self.agent_client.generate_sql("Who are our top 5 customers by total spend?")
        self.assertEqual(provider, "offline-rule-engine")
        self.assertIn("analytics.dim_customers", sql)
        self.assertIn("lifetime_value", sql)
        self.assertIn("ORDER BY lifetime_value DESC", sql)

    def test_sql_cleaning_and_safety_valid(self):
        """Test that markdown wrappers and trailing semicolons are stripped safely."""
        raw = "```sql\nSELECT * FROM analytics.fct_orders;\n```"
        cleaned = self.agent_client._clean_and_validate_sql(raw)
        self.assertEqual(cleaned, "SELECT * FROM analytics.fct_orders")

    def test_sql_safety_blocks_destructive_keywords(self):
        """Verify that destructive SQL operations are strictly blocked by the security filter."""
        destructive_queries = [
            "DROP TABLE analytics.fct_orders",
            "DELETE FROM analytics.dim_customers WHERE customer_id = '1'",
            "UPDATE analytics.dim_products SET unit_price = 0",
            "ALTER TABLE analytics.fct_orders ADD COLUMN hacked TEXT",
            "TRUNCATE TABLE analytics.fct_orders",
        ]

        for query in destructive_queries:
            with self.assertRaises(ValueError) as ctx:
                self.agent_client._clean_and_validate_sql(query)
            self.assertIn("Security Alert", str(ctx.exception))

    def test_sql_safety_requires_select_or_with(self):
        """Verify that non-query statements are rejected."""
        with self.assertRaises(ValueError) as ctx:
            self.agent_client._clean_and_validate_sql("SHOW TABLES")
        self.assertIn("Security Alert", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

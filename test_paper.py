"""Tests for quote-based paper pricing."""

import unittest

import paper


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.quote = {"depth": {
            "buy": [{"price": 97000, "quantity": 1}, {"price": 96995, "quantity": 4}],
            "sell": [{"price": 99000, "quantity": 1}, {"price": 99010, "quantity": 4}],
        }}

    def test_both_sides_and_wide_spread(self):
        buy = paper.quote_fill(self.quote, "BUY", 1)
        sell = paper.quote_fill(self.quote, "SELL", 1)
        self.assertEqual(buy["price"], 99000)
        self.assertEqual(sell["price"], 97000)
        self.assertEqual(buy["spread"], 2000)
        self.assertFalse(buy["insufficient_best_size"])

    def test_insufficient_top_size_is_recorded_not_a_filter(self):
        buy = paper.quote_fill(self.quote, "BUY", 2)
        self.assertEqual(buy["price"], 99000)
        self.assertTrue(buy["insufficient_best_size"])

    def test_invalid_or_missing_books_rejected(self):
        self.quote["depth"]["buy"] = []
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "BUY", 1)
        self.quote["depth"]["buy"] = [{"price": 100000, "quantity": 1}]
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "SELL", 1)
        self.quote["depth"]["buy"] = [{"price": float("nan"), "quantity": 1}]
        with self.assertRaises(ValueError):
            paper.quote_fill(self.quote, "BUY", 1)

if __name__ == "__main__":
    unittest.main()

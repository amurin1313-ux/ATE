import unittest
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from engine.metrics import rsi, ema, bollinger, returns_pct, slope_pct, volatility_1h_pct

class TestMetrics(unittest.TestCase):
    def test_basic(self):
        prices = [1,2,3,2,2.5,3,3.2,3.1,3.4,3.6,3.5,3.7,3.9,4.0,4.1,4.0,4.2,4.3,4.1,4.4,4.5]
        self.assertTrue(0 <= rsi(prices,14) <= 100)
        self.assertGreater(ema(prices,12), 0)
        up, lo, w = bollinger(prices,20,2)
        self.assertGreaterEqual(up, lo)
        self.assertIsInstance(returns_pct(prices,5), float)
        self.assertIsInstance(slope_pct(prices,30), float)
        prices2 = list(range(1,80))
        self.assertGreaterEqual(volatility_1h_pct(prices2), 0.0)

if __name__ == "__main__":
    unittest.main()

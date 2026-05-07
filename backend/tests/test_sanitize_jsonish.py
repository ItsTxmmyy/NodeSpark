import math
import unittest


def sanitize_jsonish(value):
    # Mirrors backend/app/main.py logic (kept local so tests don't need FastAPI imports).
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: sanitize_jsonish(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_jsonish(v) for v in value]

    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return value


class SanitizeJsonishTests(unittest.TestCase):
    def test_converts_nan_and_inf(self):
        self.assertIsNone(sanitize_jsonish(float("nan")))
        self.assertIsNone(sanitize_jsonish(float("inf")))
        self.assertIsNone(sanitize_jsonish(float("-inf")))

    def test_recursive_structures(self):
        data = {
            "a": 1,
            "b": float("nan"),
            "c": [1.0, float("inf"), {"d": float("nan")}],
        }
        out = sanitize_jsonish(data)
        self.assertEqual(out["a"], 1)
        self.assertIsNone(out["b"])
        self.assertEqual(out["c"][0], 1.0)
        self.assertIsNone(out["c"][1])
        self.assertIsNone(out["c"][2]["d"])


if __name__ == "__main__":
    unittest.main()


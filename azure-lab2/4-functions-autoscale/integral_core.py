from __future__ import annotations

import math
import time
from typing import Any, Dict, List

N_VALUES = [10, 100, 1000, 10_000, 100_000, 1_000_000]


def numerical_integral_abs_sin(lower: float, upper: float, n: int) -> float:
    """Rectangle method (left Riemann sum) for ∫ |sin(x)| dx over [lower, upper]"""
    width = (upper - lower) / n
    total = 0.0
    x = lower
    for _ in range(n):
        total += abs(math.sin(x)) * width
        x += width
    return total


def compute_integrals(lo: float, up: float) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    overall_start = time.perf_counter()

    for n in N_VALUES:
        start = time.perf_counter()
        val = numerical_integral_abs_sin(lo, up, n)
        elapsed_ms = (time.perf_counter() - start) * 1000
        results.append({"n": n, "value": val, "time_ms": elapsed_ms})

    overall_ms = (time.perf_counter() - overall_start) * 1000

    return {
        "function": "abs(sin(x))",
        "lower": lo,
        "upper": up,
        "results": results,
        "total_time_ms": overall_ms,
    }


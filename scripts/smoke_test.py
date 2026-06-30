"""
Smoke test standalone — vérifie toutes les briques d'un coup.

    docker compose run --rm airflow-scheduler python /opt/airflow/scripts/smoke_test.py
"""
from __future__ import annotations

import sys

from checks import CHECKS


def main() -> int:
    print("=== Smoke test de la stack ===\n")
    failures = 0
    for name, fn in CHECKS.items():
        try:
            msg = fn()
            print(f"[OK]   {name:6s} — {msg}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"[FAIL] {name:6s} — {type(exc).__name__}: {exc}")
    print()
    if failures:
        print(f"{failures} brique(s) KO.")
        return 1
    print("Toutes les briques répondent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

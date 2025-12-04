"""
Lightweight Oracle -> OceanBase SQL replay, compatibility, and tuning assistant.

This package focuses on orchestrating connections, running SQL on OceanBase via
`obclient`, and generating simple rule-based tuning hints.
"""

__all__ = [
    "config",
    "oracle_client",
    "oceanbase_client",
    "compatibility",
    "benchmark",
    "advisor",
    "dbreplay",
    "replay",
    "capture",
    "oma_runner",
    "cli",
]

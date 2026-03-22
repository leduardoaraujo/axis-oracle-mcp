import json
from typing import Any


def records_to_dict(columns: list[str], rows: list) -> list[dict]:
    """Convert oracledb cursor rows + column names into a list of dicts."""
    return [dict(zip(columns, row)) for row in rows]


def format_as_markdown_table(records: list[dict]) -> str:
    if not records:
        return "No records found."

    headers = list(records[0].keys())
    header_row = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
    rows = [
        "| " + " | ".join(str(r.get(h, "")) for h in headers) + " |"
        for r in records
    ]
    return "\n".join([header_row, separator] + rows)


def format_as_json(records: list[dict]) -> str:
    return json.dumps(records, indent=2, default=str)

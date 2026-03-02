#!/usr/bin/env python3
import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "qa_ingest"
TABULAR = OUT / "tabular"

CSV_FILES = [
    "Buyer interviews-Report Status - Novara.csv",
    "Decision Factors-By Buyer Interview.csv",
    "Vision for Change-Grid view.csv",
]


def normalize_row(row):
    return {k.strip(): (v or "").strip() for k, v in row.items()}


def is_blank_row(row):
    return not any((v or "").strip() for v in row.values())


def main():
    TABULAR.mkdir(parents=True, exist_ok=True)
    profiles = []
    for name in CSV_FILES:
        src = ROOT / name
        stem = Path(name).stem
        clean_name = (
            stem.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .replace("__", "_")
        )
        clean_name = f"{clean_name}.csv"
        out_path = TABULAR / clean_name

        with src.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            rows = [normalize_row(r) for r in reader if not is_blank_row(r)]

        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        profiles.append(
            {
                "source_file": name,
                "clean_file": out_path.name,
                "rows": len(rows),
                "columns": len(headers),
                "headers": headers,
            }
        )

    profile_path = OUT / "csv_profiles.json"
    with profile_path.open("w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2, ensure_ascii=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import json
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INGEST_DIR = ROOT / "qa_ingest"
CORPUS = INGEST_DIR / "corpus.jsonl"
DB_PATH = INGEST_DIR / "qa_index.db"


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript(
        """
        DROP TABLE IF EXISTS chunks;
        DROP TABLE IF EXISTS chunks_fts;

        CREATE TABLE chunks (
            chunk_id TEXT PRIMARY KEY,
            doc_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            company TEXT NOT NULL,
            contact TEXT NOT NULL,
            outcome TEXT NOT NULL,
            interview_date TEXT NOT NULL,
            tags TEXT NOT NULL,
            text TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE chunks_fts USING fts5(
            chunk_id UNINDEXED,
            doc_id UNINDEXED,
            source_type UNINDEXED,
            company UNINDEXED,
            contact UNINDEXED,
            outcome UNINDEXED,
            interview_date UNINDEXED,
            tags UNINDEXED,
            text
        );
        """
    )

    with CORPUS.open("r", encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()]

    for row in rows:
        tags = "|".join(row.get("tags", []))
        cur.execute(
            """
            INSERT INTO chunks (
                chunk_id, doc_id, source_type, company, contact, outcome, interview_date, tags, text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["chunk_id"],
                row["doc_id"],
                row["source_type"],
                row["company"],
                row["contact"],
                row["outcome"],
                row["interview_date"],
                tags,
                row["text"],
            ),
        )
        cur.execute(
            """
            INSERT INTO chunks_fts (
                chunk_id, doc_id, source_type, company, contact, outcome, interview_date, tags, text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["chunk_id"],
                row["doc_id"],
                row["source_type"],
                row["company"],
                row["contact"],
                row["outcome"],
                row["interview_date"],
                tags,
                row["text"],
            ),
        )
    con.commit()
    con.close()


if __name__ == "__main__":
    main()

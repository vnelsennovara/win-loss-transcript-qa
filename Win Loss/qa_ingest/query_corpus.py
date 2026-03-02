#!/usr/bin/env python3
import argparse
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "qa_ingest" / "qa_index.db"
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "what",
    "when",
    "where",
    "which",
    "why",
    "how",
    "did",
    "does",
    "from",
    "into",
    "about",
    "across",
}
ALIASES = {"lose": "loss", "won": "win"}


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Query transcript corpus with SQLite FTS5.")
    p.add_argument("query", help="Search question or keyword query.")
    p.add_argument("--top-k", type=int, default=8, help="Number of matches to return.")
    p.add_argument("--outcome", choices=["Win", "Loss"], help="Optional outcome filter.")
    p.add_argument(
        "--source-type",
        choices=["transcript", "summary"],
        help="Optional source type filter.",
    )
    return p


def to_fts_query(raw: str) -> str:
    terms = []
    for t in re.findall(r"[A-Za-z0-9]+", raw):
        term = t.lower()
        if len(term) < 3 or term in STOPWORDS:
            continue
        term = ALIASES.get(term, term)
        terms.append(term)
    if not terms:
        return raw.strip()
    return " OR ".join(f"{t}*" for t in terms)


def main() -> None:
    args = make_parser().parse_args()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    where = ["chunks_fts MATCH ?"]
    params = [to_fts_query(args.query)]
    if args.outcome:
        where.append("outcome = ?")
        params.append(args.outcome)
    if args.source_type:
        where.append("source_type = ?")
        params.append(args.source_type)
    params.append(args.top_k)

    sql = f"""
        SELECT
            chunk_id,
            company,
            contact,
            outcome,
            source_type,
            interview_date,
            bm25(chunks_fts) AS score,
            snippet(chunks_fts, 8, '[', ']', ' ... ', 24) AS snippet_text
        FROM chunks_fts
        WHERE {' AND '.join(where)}
        ORDER BY score
        LIMIT ?
    """
    rows = cur.execute(sql, params).fetchall()
    if not rows:
        print("No matches found.")
        return

    for i, r in enumerate(rows, start=1):
        print(
            f"{i}. {r['company']} | {r['contact']} | {r['outcome']} | {r['source_type']} | {r['interview_date']}"
        )
        print(f"   score={r['score']:.3f}")
        print(f"   {r['snippet_text']}")
        print()


if __name__ == "__main__":
    main()

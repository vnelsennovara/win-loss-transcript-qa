#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "qa_ingest" / "qa_index.db"
DEFAULT_QFILE = ROOT / "qa_ingest" / "basic_questions.json"

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
    "have",
    "has",
    "were",
    "them",
}
ALIASES = {"lose": "loss", "won": "win", "pricing": "price"}


def to_fts_query(raw: str) -> str:
    terms = []
    for tok in re.findall(r"[A-Za-z0-9]+", raw):
        term = tok.lower()
        if len(term) < 3 or term in STOPWORDS:
            continue
        term = ALIASES.get(term, term)
        terms.append(term)
    if not terms:
        return raw.strip()
    return " OR ".join(f"{t}*" for t in terms)


def fetch_matches(
    conn: sqlite3.Connection,
    query: str,
    top_k: int,
    outcome: str = "",
    source_type: str = "",
) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    where = ["chunks_fts MATCH ?"]
    params: List[object] = [to_fts_query(query)]
    if outcome:
        where.append("c.outcome = ?")
        params.append(outcome)
    if source_type:
        where.append("c.source_type = ?")
        params.append(source_type)
    params.append(top_k)
    sql = f"""
        SELECT
            c.chunk_id,
            c.doc_id,
            c.company,
            c.contact,
            c.outcome,
            c.source_type,
            c.interview_date,
            c.text,
            bm25(chunks_fts) AS score
        FROM chunks_fts
        JOIN chunks c ON c.chunk_id = chunks_fts.chunk_id
        WHERE {' AND '.join(where)}
        ORDER BY score
        LIMIT ?
    """
    return cur.execute(sql, params).fetchall()


def split_sentences(text: str) -> List[str]:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    return [p.strip() for p in parts if len(p.strip()) >= 40]


def score_sentence(sentence: str, query_terms: Sequence[str]) -> int:
    lower = sentence.lower()
    return sum(1 for t in query_terms if t and t in lower)


def extract_quotes(rows: Sequence[sqlite3.Row], question: str, n: int = 5) -> List[Dict[str, str]]:
    terms = [t.lower() for t in re.findall(r"[A-Za-z0-9]+", question) if len(t) >= 4]
    scored: List[Tuple[int, Dict[str, str]]] = []
    seen = set()
    for r in rows:
        for sentence in split_sentences(r["text"]):
            if "?" in sentence:
                continue
            low = sentence.lower()
            if low.startswith(("tell me ", "i do want to ", "i'd be interested", "and i want to talk")):
                continue
            if re.match(
                r"^(IcebergIQ|Tyler|Julie|Braxton|Travis|Richard|Danny|Brian|Patrick|Lauren|Stacy|Christopher)\b",
                sentence,
            ):
                continue
            score = score_sentence(sentence, terms)
            if score <= 0:
                continue
            key = (r["doc_id"], sentence[:90].lower())
            if key in seen:
                continue
            seen.add(key)
            scored.append(
                (
                    score,
                    {
                        "quote": sentence,
                        "company": r["company"],
                        "contact": r["contact"],
                        "outcome": r["outcome"],
                        "source_type": r["source_type"],
                        "interview_date": r["interview_date"],
                    },
                )
            )
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in scored[:n]]


def summarize(rows: Sequence[sqlite3.Row], limit: int = 5) -> List[str]:
    by_company: Dict[str, sqlite3.Row] = {}
    for r in rows:
        if r["company"] not in by_company:
            by_company[r["company"]] = r
    lines = []
    for r in list(by_company.values())[:limit]:
        snippet = " ".join(split_sentences(r["text"])[:1])[:220].strip()
        if not snippet:
            snippet = re.sub(r"\s+", " ", r["text"])[:220].strip()
        lines.append(
            f"{r['company']} ({r['outcome']}): {snippet} [source: {r['source_type']}, {r['interview_date']}]"
        )
    return lines


def best_excerpt(text: str) -> str:
    for s in split_sentences(text):
        lower = s.lower()
        if "novara |" in lower or "icebergiq" in lower:
            continue
        if lower.startswith(("okay,", "okay ", "great.", "great ")):
            continue
        if lower.startswith(("tell me ", "i do want to ", "i'd be interested")):
            continue
        return s
    fallback = re.sub(r"\s+", " ", text).strip()
    return fallback[:220]


def print_summary_answer(question: str, rows: Sequence[sqlite3.Row]) -> None:
    if not rows:
        print("No matches found.")
        return
    print(f"Question: {question}")
    print("Summary answer:")
    by_company: Dict[str, sqlite3.Row] = {}
    for r in rows:
        if r["company"] not in by_company:
            by_company[r["company"]] = r
    for r in list(by_company.values())[:5]:
        snippet = best_excerpt(r["text"])[:220]
        print(
            f"- {r['company']} ({r['outcome']}): {snippet} [source: {r['source_type']}, {r['interview_date']}]"
        )


def print_quotes_answer(question: str, rows: Sequence[sqlite3.Row]) -> None:
    if not rows:
        print("No matches found.")
        return
    print(f"Question: {question}")
    print("Supporting quotes:")
    quotes = extract_quotes(rows, question)
    if not quotes:
        print("- No high-confidence quotes found in top matches.")
        return
    for q in quotes:
        print(
            f"- \"{q['quote']}\" ({q['company']} | {q['contact']} | {q['outcome']} | {q['source_type']} | {q['interview_date']})"
        )


def run_one(
    conn: sqlite3.Connection,
    question: str,
    mode: str,
    top_k: int,
    outcome: str = "",
    source_type: str = "",
) -> Dict[str, str]:
    if mode == "summary" and not source_type:
        source_type = "summary"
    rows = fetch_matches(conn, question, top_k=top_k, outcome=outcome, source_type=source_type)
    if mode == "quotes":
        print_quotes_answer(question, rows)
        top = extract_quotes(rows, question, n=3)
        answer = " | ".join(q["quote"] for q in top) if top else "No high-confidence quotes found."
    else:
        print_summary_answer(question, rows)
        answer = " | ".join(summarize(rows, limit=3)) if rows else "No matches found."
    return {
        "question": question,
        "mode": mode,
        "outcome_filter": outcome,
        "source_type_filter": source_type,
        "answer_preview": answer[:4000],
    }


def interactive(conn: sqlite3.Connection, top_k: int) -> None:
    print("Transcript Q&A interactive mode. Type 'exit' to quit.")
    print("Format: <mode> | <question> | <optional outcome Win/Loss> | <optional source transcript/summary>")
    print("Example: quotes | top pricing objections in losses | Loss | transcript")
    while True:
        raw = input("\nqa> ").strip()
        if raw.lower() in {"exit", "quit"}:
            break
        if not raw:
            continue
        parts = [p.strip() for p in raw.split("|")]
        mode = parts[0] if parts else "summary"
        question = parts[1] if len(parts) > 1 else raw
        outcome = parts[2] if len(parts) > 2 else ""
        source_type = parts[3] if len(parts) > 3 else ""
        if mode not in {"summary", "quotes"}:
            print("Mode must be 'summary' or 'quotes'.")
            continue
        run_one(conn, question, mode, top_k, outcome=outcome, source_type=source_type)


def run_batch(
    conn: sqlite3.Connection,
    question_file: Path,
    out_csv: Path,
    top_k: int,
) -> None:
    with question_file.open("r", encoding="utf-8") as f:
        questions = json.load(f)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    rows_out = []
    for q in questions:
        result = run_one(
            conn,
            question=q["question"],
            mode=q.get("mode", "summary"),
            top_k=q.get("top_k", top_k),
            outcome=q.get("outcome", ""),
            source_type=q.get("source_type", ""),
        )
        rows_out.append(result)
        print()
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"Saved batch answers: {out_csv}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Basic Transcript Q&A tool.")
    sub = p.add_subparsers(dest="cmd", required=True)

    ask = sub.add_parser("ask", help="Ask one question.")
    ask.add_argument("--question", required=True)
    ask.add_argument("--mode", choices=["summary", "quotes"], default="summary")
    ask.add_argument("--top-k", type=int, default=20)
    ask.add_argument("--outcome", choices=["Win", "Loss"], default="")
    ask.add_argument("--source-type", choices=["transcript", "summary"], default="")

    inter = sub.add_parser("interactive", help="Interactive QA mode.")
    inter.add_argument("--top-k", type=int, default=20)

    batch = sub.add_parser("run-questions", help="Run pre-generated questions from JSON.")
    batch.add_argument("--question-file", type=Path, default=DEFAULT_QFILE)
    batch.add_argument("--out-csv", type=Path, default=ROOT / "qa_ingest" / "answers.csv")
    batch.add_argument("--top-k", type=int, default=20)
    return p


def main() -> None:
    args = build_parser().parse_args()
    conn = sqlite3.connect(DB_PATH)
    try:
        if args.cmd == "ask":
            run_one(
                conn,
                question=args.question,
                mode=args.mode,
                top_k=args.top_k,
                outcome=args.outcome,
                source_type=args.source_type,
            )
            return
        if args.cmd == "interactive":
            interactive(conn, top_k=args.top_k)
            return
        if args.cmd == "run-questions":
            run_batch(
                conn,
                question_file=args.question_file,
                out_csv=args.out_csv,
                top_k=args.top_k,
            )
            return
    finally:
        conn.close()


if __name__ == "__main__":
    main()

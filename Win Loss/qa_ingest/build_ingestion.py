#!/usr/bin/env python3
import csv
import json
import re
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "qa_ingest"
SUMMARY_DOC = ROOT / "Summary Documents.docx"


TAG_RULES = {
    "pricing": ["price", "pricing", "budget", "cost", "implementation fee", "quote"],
    "implementation": ["implementation", "onboarding", "go-live", "rollout", "kickoff"],
    "support": ["support", "account manager", "responsive", "customer success", "help"],
    "training_content": ["training", "content", "video", "library", "lms"],
    "compliance": ["osha", "iso", "compliance", "regulatory", "audit"],
    "incident_management": ["incident", "investigation", "recordable", "trir", "dart"],
    "sds": ["sds", "chemical", "msds"],
    "reporting_analytics": ["dashboard", "reporting", "kpi", "metrics", "excel"],
    "customization": ["custom", "customize", "forms", "workflow", "flex"],
    "usability_mobile": ["easy to use", "user-friendly", "mobile", "qr"],
}


@dataclass
class TranscriptRecord:
    doc_id: str
    file_name: str
    company: str
    contact: str
    outcome: str
    loss_detail: str
    interview_date: str
    transcript_words: int
    transcript_chars: int
    summary_found: bool
    summary_words: int
    summary_chars: int
    tags: str


def normalize_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def run_textutil_to_txt(path: Path) -> str:
    proc = subprocess.run(
        ["textutil", "-convert", "txt", "-stdout", str(path)],
        check=True,
        text=True,
        capture_output=True,
    )
    return proc.stdout


def parse_transcript_filename(path: Path) -> Optional[Tuple[str, str, str, str, str]]:
    stem = path.stem
    if not stem.startswith("Novara _ "):
        return None
    body = stem[len("Novara _ ") :]
    m = re.match(
        r"^(?P<company>.+?) (?P<outcome>Win|Loss)(?P<loss_detail> to [A-Za-z ]+)? (?P<contact>[A-Za-z0-9.,'&-]+(?: [A-Za-z0-9.,'&-]+)+) (?P<date>\d{1,2}[A-Za-z]{3}\d{4})$",
        body,
    )
    if not m:
        return None
    company = m.group("company").strip()
    outcome = m.group("outcome").strip()
    loss_detail = (m.group("loss_detail") or "").strip()
    contact = m.group("contact").strip()
    date_raw = m.group("date")
    interview_date = datetime.strptime(date_raw, "%d%b%Y").date().isoformat()
    return company, outcome, loss_detail, contact, interview_date


def parse_summary_sections(summary_text: str) -> Dict[str, Dict[str, str]]:
    lines = [ln.rstrip() for ln in summary_text.splitlines()]
    sections: List[Tuple[str, str, str]] = []
    current_header = None
    current_body: List[str] = []

    header_re = re.compile(
        r"^(?:An|n) interview has been conducted with (?P<contact>.+?) from (?P<company>.+?) on .+\.$",
        flags=re.IGNORECASE,
    )

    def flush():
        nonlocal current_header, current_body
        if current_header:
            body = "\n".join(current_body).strip()
            sections.append((current_header[0], current_header[1], body))
        current_header = None
        current_body = []

    for line in lines:
        m = header_re.match(line.strip())
        if m:
            flush()
            current_header = (m.group("company").strip(), m.group("contact").strip())
            continue
        if current_header is not None:
            current_body.append(line)
    flush()

    out: Dict[str, Dict[str, str]] = {}
    for company, contact, body in sections:
        cleaned = re.sub(r"^Summary:\s*", "", body.strip(), flags=re.IGNORECASE)
        key = normalize_key(company)
        out[key] = {"company": company, "contact": contact, "text": cleaned.strip()}
    return out


def extract_tags(*texts: str) -> List[str]:
    haystack = " ".join(texts).lower()
    tags = []
    for tag, terms in TAG_RULES.items():
        if any(term in haystack for term in terms):
            tags.append(tag)
    return tags


def chunk_words(text: str, size: int = 220, overlap: int = 40) -> List[str]:
    words = text.split()
    if not words:
        return []
    chunks = []
    i = 0
    while i < len(words):
        chunk = words[i : i + size]
        chunks.append(" ".join(chunk).strip())
        if i + size >= len(words):
            break
        i += max(1, size - overlap)
    return chunks


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    transcript_files = sorted(
        p
        for p in ROOT.glob("Novara _ *.docx")
        if p.name != "Summary Documents.docx"
    )

    summary_text = run_textutil_to_txt(SUMMARY_DOC) if SUMMARY_DOC.exists() else ""
    summaries = parse_summary_sections(summary_text)

    metadata_rows: List[TranscriptRecord] = []
    corpus_rows: List[dict] = []

    for path in transcript_files:
        parsed = parse_transcript_filename(path)
        if not parsed:
            continue
        company, outcome, loss_detail, contact, interview_date = parsed
        transcript_text = run_textutil_to_txt(path).strip()
        transcript_words = len(transcript_text.split())
        transcript_chars = len(transcript_text)
        key = normalize_key(company)

        summary = summaries.get(key)
        summary_text_match = summary["text"].strip() if summary else ""
        tags = extract_tags(transcript_text, summary_text_match)

        doc_id = normalize_key(f"{company}_{contact}_{interview_date}")
        metadata_rows.append(
            TranscriptRecord(
                doc_id=doc_id,
                file_name=path.name,
                company=company,
                contact=contact,
                outcome=outcome,
                loss_detail=loss_detail,
                interview_date=interview_date,
                transcript_words=transcript_words,
                transcript_chars=transcript_chars,
                summary_found=bool(summary_text_match),
                summary_words=len(summary_text_match.split()),
                summary_chars=len(summary_text_match),
                tags="|".join(tags),
            )
        )

        transcript_chunks = chunk_words(transcript_text)
        for idx, chunk in enumerate(transcript_chunks, start=1):
            corpus_rows.append(
                {
                    "chunk_id": f"{doc_id}_transcript_{idx:03d}",
                    "doc_id": doc_id,
                    "source_type": "transcript",
                    "company": company,
                    "contact": contact,
                    "outcome": outcome,
                    "interview_date": interview_date,
                    "tags": tags,
                    "text": chunk,
                }
            )

        if summary_text_match:
            summary_chunks = chunk_words(summary_text_match, size=180, overlap=30)
            for idx, chunk in enumerate(summary_chunks, start=1):
                corpus_rows.append(
                    {
                        "chunk_id": f"{doc_id}_summary_{idx:03d}",
                        "doc_id": doc_id,
                        "source_type": "summary",
                        "company": company,
                        "contact": contact,
                        "outcome": outcome,
                        "interview_date": interview_date,
                        "tags": tags,
                        "text": chunk,
                    }
                )

    csv_path = OUT_DIR / "metadata.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(metadata_rows[0]).keys()))
        writer.writeheader()
        for row in metadata_rows:
            writer.writerow(asdict(row))

    jsonl_path = OUT_DIR / "corpus.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for row in corpus_rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    matched_keys = {normalize_key(r.company) for r in metadata_rows}
    unmatched_summary = [
        s for k, s in summaries.items() if k not in matched_keys
    ]
    report_path = OUT_DIR / "ingestion_report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Ingestion Report\n\n")
        f.write(f"- Transcripts ingested: {len(metadata_rows)}\n")
        f.write(f"- Corpus chunks written: {len(corpus_rows)}\n")
        f.write(
            f"- Transcript records with summary match: {sum(1 for r in metadata_rows if r.summary_found)}\n"
        )
        f.write(
            f"- Transcript records missing summary: {sum(1 for r in metadata_rows if not r.summary_found)}\n"
        )
        f.write(f"- Summary sections without matching transcript: {len(unmatched_summary)}\n\n")
        if unmatched_summary:
            f.write("## Unmatched Summary Sections\n")
            for entry in unmatched_summary:
                f.write(f"- {entry['company']} ({entry['contact']})\n")


if __name__ == "__main__":
    main()

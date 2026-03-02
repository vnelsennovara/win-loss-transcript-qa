# Transcript Q&A Ingestion Outputs

## Generated Files
- `metadata.csv`: One row per transcript with parsed metadata, lengths, tags, and summary match status.
- `corpus.jsonl`: Chunked transcript + summary text records for retrieval/QA.
- `ingestion_report.md`: Coverage report with unmatched summary/transcript notes.
- `csv_profiles.json`: Schema/row profiles for source CSV files.
- `tabular/*.csv`: Cleaned CSV copies (UTF-8, blank-row filtered).
- `qa_index.db`: Local SQLite FTS5 search index over `corpus.jsonl`.

## Rebuild Commands
```bash
python3 qa_ingest/build_ingestion.py
python3 qa_ingest/organize_csvs.py
python3 qa_ingest/build_search_index.py
```

## Ask Questions (Local Search)
```bash
python3 qa_ingest/query_corpus.py "top reasons we lost to competitors" --top-k 8
python3 qa_ingest/query_corpus.py "implementation onboarding timeline" --source-type summary --top-k 8
python3 qa_ingest/query_corpus.py "pricing objections" --outcome Loss --top-k 10
```

## Basic Q&A Tool (Summary + Quotes)
```bash
# one question
python3 qa_ingest/qa_tool.py ask --mode summary --question "top reasons we lose" --outcome Loss
python3 qa_ingest/qa_tool.py ask --mode quotes --question "pricing concerns" --source-type transcript

# interactive mode for teammates
python3 qa_ingest/qa_tool.py interactive

# batch mode from pre-generated list
python3 qa_ingest/qa_tool.py run-questions --question-file qa_ingest/basic_questions.json --out-csv qa_ingest/answers.csv
```

Batch question format (`basic_questions.json`):
- `question` (required)
- `mode`: `summary` or `quotes`
- `outcome`: `Win` or `Loss` (optional)
- `source_type`: `transcript` or `summary` (optional)
- `top_k` (optional)

## No-Terminal Launch (Mac)
- Double-click: `/Users/vanessanelsen/Desktop/Win Loss/Open Transcript QA.command`
- This opens a simple desktop Q&A window.

#!/usr/bin/env python3
import sqlite3
from pathlib import Path
import sys

import streamlit as st


ROOT = Path(__file__).resolve().parent
QA_DIR = ROOT / "qa_ingest"
DB_PATH = QA_DIR / "qa_index.db"

sys.path.append(str(QA_DIR))
from qa_tool import fetch_matches, summarize, extract_quotes  # noqa: E402
from build_search_index import main as build_index  # noqa: E402


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        build_index()
    return sqlite3.connect(DB_PATH)


st.set_page_config(page_title="Transcript Q&A", layout="wide")
st.title("Transcript Q&A")
st.caption("Ask summary questions across transcripts and pull supporting quotes.")

with st.sidebar:
    st.header("Options")
    mode = st.selectbox("Mode", ["summary", "quotes"], index=0)
    outcome = st.selectbox("Outcome filter", ["", "Win", "Loss"], index=0)
    source = st.selectbox("Source filter", ["", "summary", "transcript"], index=0)
    top_k = st.slider("Top K matches", 5, 60, 30, 5)
    st.markdown("---")
    st.write("Tip: summary mode works best with high-level questions.")

question = st.text_area(
    "Question",
    placeholder="Example: What are top pricing objections in loss interviews?",
    height=100,
)

if st.button("Ask", type="primary", use_container_width=True):
    if not question.strip():
        st.warning("Enter a question first.")
        st.stop()

    conn = get_conn()
    use_source = source
    if mode == "summary" and not use_source:
        use_source = "summary"

    rows = fetch_matches(
        conn,
        question.strip(),
        top_k=top_k,
        outcome=outcome,
        source_type=use_source,
    )
    if not rows:
        st.info("No matches found.")
        st.stop()

    if mode == "summary":
        st.subheader("Summary Answer")
        for line in summarize(rows, limit=8):
            st.markdown(f"- {line}")
    else:
        st.subheader("Supporting Quotes")
        quotes = extract_quotes(rows, question.strip(), n=10)
        if not quotes:
            st.info("No high-confidence quotes found in top matches.")
        for q in quotes:
            st.markdown(
                f"- \"{q['quote']}\"\n  \n"
                f"  `{q['company']} | {q['contact']} | {q['outcome']} | {q['source_type']} | {q['interview_date']}`"
            )

    with st.expander("Show raw retrieval matches"):
        for r in rows[:12]:
            st.markdown(
                f"**{r['company']} | {r['contact']} | {r['outcome']} | {r['source_type']} | {r['interview_date']}**"
            )
            st.write(r["text"][:700] + ("..." if len(r["text"]) > 700 else ""))

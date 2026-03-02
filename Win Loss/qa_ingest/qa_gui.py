#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "qa_ingest") not in sys.path:
    sys.path.append(str(ROOT / "qa_ingest"))

from qa_tool import fetch_matches, summarize, extract_quotes  # noqa: E402
import sqlite3  # noqa: E402


DB_PATH = ROOT / "qa_ingest" / "qa_index.db"


class QAGui:
    def __init__(self, master: tk.Tk):
        self.master = master
        self.master.title("Transcript Q&A")
        self.master.geometry("980x680")
        self.conn = sqlite3.connect(DB_PATH)

        frame = ttk.Frame(master, padding=12)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Question").grid(row=0, column=0, sticky="w")
        self.question = tk.Text(frame, height=3, width=110)
        self.question.grid(row=1, column=0, columnspan=6, sticky="ew", pady=(4, 10))

        ttk.Label(frame, text="Mode").grid(row=2, column=0, sticky="w")
        self.mode = ttk.Combobox(frame, values=["summary", "quotes"], state="readonly", width=14)
        self.mode.set("summary")
        self.mode.grid(row=3, column=0, sticky="w", padx=(0, 10))

        ttk.Label(frame, text="Outcome").grid(row=2, column=1, sticky="w")
        self.outcome = ttk.Combobox(frame, values=["", "Win", "Loss"], state="readonly", width=14)
        self.outcome.set("")
        self.outcome.grid(row=3, column=1, sticky="w", padx=(0, 10))

        ttk.Label(frame, text="Source").grid(row=2, column=2, sticky="w")
        self.source = ttk.Combobox(frame, values=["", "summary", "transcript"], state="readonly", width=14)
        self.source.set("")
        self.source.grid(row=3, column=2, sticky="w", padx=(0, 10))

        ttk.Label(frame, text="Top K").grid(row=2, column=3, sticky="w")
        self.top_k = ttk.Entry(frame, width=8)
        self.top_k.insert(0, "30")
        self.top_k.grid(row=3, column=3, sticky="w", padx=(0, 10))

        ttk.Button(frame, text="Ask", command=self.run_query).grid(row=3, column=4, sticky="w")
        ttk.Button(frame, text="Clear", command=self.clear_output).grid(row=3, column=5, sticky="w")

        ttk.Label(frame, text="Answer").grid(row=4, column=0, sticky="w", pady=(12, 4))
        self.output = tk.Text(frame, height=24, width=110, wrap="word")
        self.output.grid(row=5, column=0, columnspan=6, sticky="nsew")

        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(5, weight=1)

    def clear_output(self):
        self.output.delete("1.0", tk.END)

    def run_query(self):
        q = self.question.get("1.0", tk.END).strip()
        if not q:
            return
        try:
            top_k = int(self.top_k.get().strip())
        except ValueError:
            top_k = 30
        mode = self.mode.get() or "summary"
        outcome = self.outcome.get().strip()
        source = self.source.get().strip()
        if mode == "summary" and not source:
            source = "summary"

        rows = fetch_matches(self.conn, q, top_k=top_k, outcome=outcome, source_type=source)
        self.output.delete("1.0", tk.END)
        self.output.insert(tk.END, f"Question: {q}\n\n")
        if not rows:
            self.output.insert(tk.END, "No matches found.")
            return

        if mode == "summary":
            self.output.insert(tk.END, "Summary answer:\n")
            for line in summarize(rows, limit=6):
                self.output.insert(tk.END, f"- {line}\n")
        else:
            self.output.insert(tk.END, "Supporting quotes:\n")
            quotes = extract_quotes(rows, q, n=8)
            if not quotes:
                self.output.insert(tk.END, "- No high-confidence quotes found.\n")
                return
            for item in quotes:
                self.output.insert(
                    tk.END,
                    f"- \"{item['quote']}\" ({item['company']} | {item['contact']} | {item['outcome']} | {item['source_type']} | {item['interview_date']})\n",
                )


def main():
    root = tk.Tk()
    app = QAGui(root)
    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    app.conn.close()


if __name__ == "__main__":
    main()

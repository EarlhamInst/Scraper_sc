import re
import logging
import argparse
from pathlib import Path
import json
import shutil

import fitz  # PyMuPDF
import pandas as pd

# ---------------------------
# Logging setup
# ---------------------------
def setup_logger(verbosity: int, log_file: str | None):
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    datefmt = "%H:%M:%S"

    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)


# ---------------------------
# Configurable patterns
# ---------------------------
PATTERNS = [
    ("ENA Project", r"(PRJ[EDN][A-Z]?\d{4,})"),
    ("GEO Series",  r"(GSE\d{4,})"),
    ("GEO Sample",  r"(GSM\d{4,})"),
    ("Biosample",   r"(SAM[END][A-Z]?\d{4,})"),
]
COMPILED = [(name, re.compile(rx, re.IGNORECASE)) for name, rx in PATTERNS]


# ---------------------------
# PDF utilities
# ---------------------------
def extract_pdf_text(pdf_path: Path) -> str:
    logging.debug(f"Extracting text: {pdf_path.name}")
    doc = fitz.open(pdf_path)
    try:
        text = "\n".join(page.get_text() for page in doc)
        logging.debug(f"Extracted {len(text)} characters from {pdf_path.name}")
        return text
    finally:
        doc.close()


def clean_title(t: str) -> str:
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[\s\-–—:]+$", "", t)
    return t


def guess_title_from_first_page(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    try:
        meta_title = (doc.metadata or {}).get("title", "") or ""
        if meta_title and meta_title.strip() and len(meta_title.strip()) > 3:
            title = clean_title(meta_title)
            logging.debug(f"Title from metadata: {title}")
            return title

        if doc.page_count > 0:
            page = doc.load_page(0)
            d = page.get_text("dict")
            best = {"size": -1.0, "text": ""}
            for block in d.get("blocks", []):
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        txt = (span.get("text") or "").strip()
                        size = float(span.get("size") or 0.0)
                        if txt and len(txt) > 5 and not txt.isupper():
                            if size > best["size"]:
                                best = {"size": size, "text": txt}
            if best["text"]:
                title = clean_title(best["text"])
                logging.debug(f"Title from largest span: {title} (size {best['size']})")
                return title

        # Fallback: first non-empty line
        if doc.page_count > 0:
            raw = doc.load_page(0).get_text()
            for line in raw.splitlines():
                L = line.strip()
                if L and len(L) > 5:
                    title = clean_title(L)
                    logging.debug(f"Title from first non-empty line: {title}")
                    return title

        logging.debug("No reliable title found; using file stem")
        return pdf_path.stem
    finally:
        doc.close()


# ---------------------------
# Accession extraction
# ---------------------------
def find_accessions(text: str) -> list[tuple[str, str]]:
    found = []
    seen = set()
    # Also search de-hyphenated version to catch line breaks splitting IDs
    text_dehyphen = re.sub(r"(\w)-\n(\w)", r"\1\2", text, flags=re.MULTILINE)
    targets = [text, text_dehyphen]

    for acc_type, rx in COMPILED:
        for target in targets:
            for m in rx.finditer(target):
                acc = m.group(1).upper()
                key = (acc_type, acc)
                if key not in seen:
                    seen.add(key)
                    found.append((acc_type, acc))
    return found


# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract paper titles and archive accessions from PDFs."
    )
    parser.add_argument("--input-dir", default="pdfs", type=str,
                        help="Directory containing PDFs (default: pdfs)")
    parser.add_argument("--output-csv", default="accessions.csv", type=str,
                        help="Path to output CSV (default: accessions.csv)")
    parser.add_argument("--output-jsonl", default="accessions.jsonl", type=str,
                        help="Path to output JSONL (default: accessions.jsonl)")
    parser.add_argument("--no-jsonl", action="store_true",
                        help="Disable JSONL output")
    parser.add_argument("--move-done", action="store_true",
                        help="Move processed PDFs to ./done")
    parser.add_argument("-v", "--verbose", action="count", default=1,
                        help="Increase verbosity: -v for INFO, -vv for DEBUG")
    parser.add_argument("--log-file", type=str, default=None,
                        help="Optional log file path")
    args = parser.parse_args()

    setup_logger(args.verbose, args.log_file)

    input_dir = Path(args.input_dir)
    output_csv = Path(args.output_csv)
    output_jsonl = Path(args.output_jsonl)
    write_jsonl = not args.no_jsonl

    logging.info("Starting accession extraction run")
    logging.info(f"Input directory: {input_dir.resolve()}")
    logging.info(f"Output CSV: {output_csv.resolve()}")
    if write_jsonl:
        logging.info(f"Output JSONL: {output_jsonl.resolve()}")
    if args.move_done:
        logging.info("Processed PDFs will be moved to ./done")

    input_dir.mkdir(parents=True, exist_ok=True)
    pdf_files = sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"])
    if not pdf_files:
        logging.warning("No PDFs found. Exiting.")
        return

    logging.info(f"Found {len(pdf_files)} PDF(s) to process")
    rows = []
    jsonl_records = []
    total_matches = 0

    for idx, pdf in enumerate(pdf_files, start=1):
        logging.info(f"[{idx}/{len(pdf_files)}] Processing: {pdf.name}")
        try:
            title = guess_title_from_first_page(pdf)
            text = extract_pdf_text(pdf)
            matches = find_accessions(text)

            if matches:
                logging.info(f"  → {len(matches)} accession(s) found")
                for acc_type, acc in matches:
                    logging.debug(f"    {acc_type}: {acc}")
                total_matches += len(matches)
                for acc_type, acc in matches:
                    rows.append({
                        "pdf_file": pdf.name,
                        "paper_title": title,
                        "accession_type": acc_type,
                        "accession": acc
                    })
                if write_jsonl:
                    jsonl_records.append({
                        "pdf_file": pdf.name,
                        "paper_title": title,
                        "accessions": [{"type": t, "id": a} for t, a in matches]
                    })
            else:
                logging.warning("  → No accessions found")
                rows.append({
                    "pdf_file": pdf.name,
                    "paper_title": title,
                    "accession_type": "",
                    "accession": ""
                })
                if write_jsonl:
                    jsonl_records.append({
                        "pdf_file": pdf.name,
                        "paper_title": title,
                        "accessions": []
                    })

            if args.move_done:
                done_path = Path("done") / pdf.name
                done_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(pdf), str(done_path))
                logging.info(f"  → Moved to {done_path}")

        except Exception as e:
            logging.exception(f"  ! Error processing {pdf.name}: {e}")
            rows.append({
                "pdf_file": pdf.name,
                "paper_title": f"ERROR: {e.__class__.__name__}",
                "accession_type": "",
                "accession": ""
            })
            if write_jsonl:
                jsonl_records.append({
                    "pdf_file": pdf.name,
                    "paper_title": f"ERROR: {e.__class__.__name__}",
                    "accessions": [],
                    "error": str(e)
                })

    # Write outputs
    df = pd.DataFrame(rows, columns=["pdf_file", "paper_title", "accession_type", "accession"])
    df.to_csv(output_csv, index=False)
    logging.info(f"Wrote CSV: {output_csv} ({len(df)} rows; {total_matches} total accessions)")

    if write_jsonl:
        with output_jsonl.open("w", encoding="utf-8") as f:
            for obj in jsonl_records:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        logging.info(f"Wrote JSONL: {output_jsonl} ({len(jsonl_records)} record(s))")

    logging.info("Run complete")


if __name__ == "__main__":
    main()

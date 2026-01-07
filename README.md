# 🧬 Single-Cell Metadata Extractor with GPT

This project automates the extraction of structured metadata from scientific papers in PDF format. Using the OpenAI GPT model, it populates a standardised Excel workbook with metadata for single-cell RNA-seq studies. Each PDF is parsed and its contents used to fill out the appropriate fields across multiple Excel sheets, one per metadata category.

---

## 📁 Project Structure

```plaintext
.
├── pdfs/                     # Folder containing input PDF files
├── completed_manifests/     # Output folder for generated Excel files
├── done/                    # Archive for processed PDF files
├── sc_rnaseq_mixs_v0.1_base_unprotected.xlsx  # Base Excel template
├── extract_metadata.py      # Main script
├── README.md                # This file

⚙️ Requirements

    Python 3.8+

    Dependencies:

        openai

        pandas

        openpyxl

        PyMuPDF (install via pip install pymupdf)

Install everything with:

pip install -r requirements.txt

🔑 Environment Variables

Set your OpenAI API key in your environment:

export GPT_KEY=your-openai-api-key

🚀 Usage

    Prepare PDFs: Place your scientific paper PDFs in the pdfs/ directory.

    Ensure base Excel file is present: The sc_rnaseq_mixs_v0.1_base_unprotected.xlsx template should be in the root directory.

    Run the script:

python extract_metadata.py

The script will:

    Extract text from each PDF.

    Use OpenAI GPT to extract metadata for each worksheet (study, person, sample, etc.).

    Write results into a new Excel file in completed_manifests/.

    Move the original PDF to done/ when finished.

🧠 Metadata Context & GPT Prompting

GPT is prompted with detailed domain-specific context for single-cell genomics. Each worksheet is filled by asking GPT to extract required fields from the full text of the paper. The script ensures:

    One row per item (no arrays).

    Optional fields may be blank; required ones are prioritised.

    Unique IDs are created and preserved across sheets.

🛠️ Notes & Tips

    If the script fails to parse the GPT output as JSON, it will still produce a row of placeholder data.

    Sheet column widths are auto-adjusted for readability.

    Sheets are marked visible in the output workbook.

    Only sheets present in the template will be processed.

    GPT model used: gpt-4o ("o3" alias).
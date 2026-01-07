
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from time import sleep

# ========== CONFIGURATION ==========
EMAIL = "felix.shaw@earlham.ac.uk"  # REQUIRED for Unpaywall
OUTPUT_DIR = "pdfs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DOIS = [
    "10.3389/fpls.2024.1274013",
    "10.1186/s13059-023-02908-x",
    "10.1016/j.xplc.2023.100740",
    "10.1016/j.plantsci.2022.111535",
    "10.1016/j.devcel.2020.12.015",
    "10.1104/pp.18.01482",
    "10.1111/pce.14906",
    "10.1038/s41477-023-01387-z",
    "10.1016/j.molp.2020.06.010",
    "10.1038/s41467-025-55870-6",
    "10.1038/s41477-023-01567-x",
    "10.1093/plphys/kiab489",
    "10.3390/ijms23052759",
    "10.1038/s41477-022-01178-y"
]

HEADERS = {"User-Agent": "Mozilla/5.0"}


# ========== STEP 1: Try Unpaywall ==========
def get_pdf_url_unpaywall(doi):
    try:
        url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={EMAIL}"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            oa = data.get("best_oa_location")
            if oa and oa.get("url_for_pdf"):
                return oa["url_for_pdf"]
    except Exception as e:
        print(f"❌ Unpaywall error for {doi}: {e}")
    return None


# ========== STEP 2: Try to extract PDF link from publisher site ==========
def get_pdf_link_from_doi_page(doi):
    doi_url = f"https://doi.org/{doi}"
    try:
        r = requests.get(doi_url, headers=HEADERS, timeout=15, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()
            if "pdf" in href.lower() or "download" in text:
                pdf_url = urljoin(r.url, href)
                return pdf_url
    except Exception as e:
        print(f"❌ Failed to parse publisher page for {doi}: {e}")
    return None


# ========== STEP 3: Download PDF ==========
def download_pdf(pdf_url, output_path):
    try:
        r = requests.get(pdf_url, headers=HEADERS, stream=True, timeout=20)
        print(r.history)
        if r.status_code == 200 and 'application/pdf' in r.headers.get("Content-Type", ""):
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(4096):
                    f.write(chunk)
            print(f"✅ Downloaded: {output_path}")
            return True
        else:
            print(f"⚠️ Not a PDF or blocked: {pdf_url}")
    except Exception as e:
        print(f"❌ Download failed from {pdf_url}: {e}")
    return False


# ========== MAIN LOOP ==========
for doi in DOIS:
    print(f"\n🔍 Processing DOI: {doi}")
    filename = os.path.join(OUTPUT_DIR, doi.replace('/', '_') + ".pdf")

    # Try Unpaywall
    pdf_url = get_pdf_url_unpaywall(doi)
    if pdf_url:
        print(f"📦 Found via Unpaywall: {pdf_url}")
        if download_pdf(pdf_url, filename):
            continue  # Success

    # Try fallback via publisher site
    pdf_url = get_pdf_link_from_doi_page(doi)
    if pdf_url:
        print(f"📦 Found on publisher site: {pdf_url}")
        download_pdf(pdf_url, filename)
    else:
        print(f"❌ No PDF found for {doi}")

    sleep(1)  # Polite delay
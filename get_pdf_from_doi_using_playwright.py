import os
import re

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    raise ImportError(
        "The 'playwright' module is not installed. Please install it with:\n"
        "    pip install playwright\n"
        "    playwright install"
    )

# ---------------------------------------------------------------------------
#  Configuration
# ---------------------------------------------------------------------------

# List of DOIs to download
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
    "10.1038/s41477-022-01178-y",
]

# If you want to test with a single DOI uncomment the next line
#DOIS = ["10.1186/s13059-023-02908-x"]

# Output locations
OUTPUT_DIR = "pdfs"
SCREENSHOT_DIR = "screenshots"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
#  Selectors
# ---------------------------------------------------------------------------

# Elements that (when clicked) should start a PDF download
PDF_SELECTORS = [
    'a:has-text("Download PDF")',
    'a:has-text("PDF")',
    'a:has-text("Full Text PDF")',
    'a[href$=".pdf"]',
    'button:has-text("PDF")',
    'a.pdf-link',
    'a#pdfLink',
    'button:has-text("Download")',
    'a:has-text("Download")',
    'button[aria-label*="PDF"]',
    'span:has-text("Download PDF")',
]

# PDF viewers embedded in the page
VIEWER_SELECTORS = [
    'iframe[src*=".pdf"]',
    'iframe[src*="download"]',
    '[class*="pdf-viewer"]',
    'embed[type="application/pdf"]',
]

# Buttons commonly used by consent managers to “allow/accept” cookies
COOKIE_ACCEPT_SELECTORS = [
    '#onetrust-accept-btn-handler',             # OneTrust
    'button:has-text("Accept")',
    'button:has-text("Accept All")',
    'button:has-text("Accept all")',
    'button:has-text("Allow all")',
    'button:has-text("Allow All")',
    'button:has-text("Allow cookies")',
    'button:has-text("Accept Cookies")',
    'button:has-text("I agree")',
    'button:has-text("I Agree")',
    'button#accept-cookies',
    'button.cookie-accept',
    '[data-track-action="download pdf"]',
]

# ---------------------------------------------------------------------------
#  Helper functions
# ---------------------------------------------------------------------------

def make_safe_filename(s: str) -> str:
    """Remove characters that are illegal in filenames."""
    return re.sub(r"[^\w\-_.]", "_", s)


def _accept_cookie_banner(page) -> None:
    """
    Click the first visible “accept/allow cookies” button found
    on the main page or in any iframe.
    """
    for frame in [page, *page.frames]:
        for sel in COOKIE_ACCEPT_SELECTORS:
            try:
                locator = frame.locator(sel).first
                if locator.is_visible(timeout=1500):
                    print(f"🍪  Accepting cookies with selector: {sel}")
                    locator.click(force=True, timeout=2000)
                    page.wait_for_timeout(1000)  # give banner time to vanish
                    return
            except Exception:
                # Selector not found / not visible in this frame
                pass


# ---------------------------------------------------------------------------
#  Main routine
# ---------------------------------------------------------------------------

def download_pdf_with_playwright(doi: str) -> None:
    """
    Navigate to a DOI landing page and try to obtain the corresponding PDF,
    accepting cookie banners automatically along the way.
    """
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        # Make sure we try to accept cookies after each navigation
        page.on("framenavigated", lambda _: _accept_cookie_banner(page))
        page.on("load", lambda _: _accept_cookie_banner(page))

        intercepted_pdf_urls = []

        # Intercept network requests to spot PDFs
        def log_pdf_requests(route, request):
            url = request.url
            if ".pdf" in url and request.resource_type == "document":
                intercepted_pdf_urls.append(url)
            route.continue_()

        context.route("**/*", log_pdf_requests)

        doi_url = f"https://doi.org/{doi}"
        safe_filename = make_safe_filename(doi)

        try:
            print(f"\n🌐  Navigating to: {doi_url}")
            page.goto(doi_url, timeout=60_000)
            _accept_cookie_banner(page)           # first attempt immediately
        except Exception as e:
            print(f"❌ Failed to load DOI page: {e}")
            browser.close()
            return

        # -------------------------------------------------------------------
        # 1. Try to click explicit “download PDF” links/buttons
        # -------------------------------------------------------------------
        download_success = False
        for selector in PDF_SELECTORS:
            try:
                locator = page.locator(selector).first
                if locator.is_visible(timeout=3_000):
                    print(f"🔍 Found clickable PDF element: {selector}")
                    with page.expect_download(timeout=10_000) as download_info:
                        locator.click(force=True)
                    download = download_info.value
                    pdf_path = os.path.join(OUTPUT_DIR, f"{safe_filename}.pdf")
                    download.save_as(pdf_path)
                    print(f"✅ Downloaded PDF to {pdf_path}")
                    download_success = True
                    break
            except Exception:
                # Not visible / click failed / no download → try next selector
                continue

        # -------------------------------------------------------------------
        # 2. If nothing was clicked, check if a PDF URL was intercepted
        # -------------------------------------------------------------------
        if not download_success and intercepted_pdf_urls:
            pdf_url = intercepted_pdf_urls[0]
            try:
                print(f"📡  Attempting to fetch intercepted PDF URL: {pdf_url}")
                with page.expect_download(timeout=10_000) as download_info:
                    page.goto(pdf_url)
                download = download_info.value
                pdf_path = os.path.join(OUTPUT_DIR, f"{safe_filename}.pdf")
                download.save_as(pdf_path)
                print(f"✅ Downloaded PDF from viewer to {pdf_path}")
                download_success = True
            except Exception as e:
                print(f"⚠️  Intercepted URL fetch failed: {e}")

        # -------------------------------------------------------------------
        # 3. If still unsuccessful, take a full-page screenshot for inspection
        # -------------------------------------------------------------------
        if not download_success:
            screenshot_path = os.path.join(
                SCREENSHOT_DIR, f"{safe_filename}.png"
            )
            try:
                page.screenshot(path=screenshot_path, full_page=True)
                print(f"❌ PDF not found. 📸 Screenshot saved to {screenshot_path}")
            except Exception as e:
                print(e)
        browser.close()


# ---------------------------------------------------------------------------
#  Run for all DOIs
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    for doi in DOIS:
        download_pdf_with_playwright(doi)
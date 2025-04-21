import imaplib
import email
import re
from urllib.parse import urlparse
import requests
import sqlite3
import time
from pathlib import Path
import sys
import json
import bsdiff4
import fitz


# --- Configuration ---
IMAP_SERVER = (
    "imap.gmail.com"  # Replace with your IMAP server address (e.g., imap.gmail.com)
)
EMAIL_ADDRESS = "user"  # Replace with your email address or IMAP user
PASSWORD = "pass"  # Replace with your email password
# The mailbox to check (e.g., "INBOX", "Sent") If spaces in name, use "" i.e.: '"mail box"'
MAILBOX = 'INBOX/Some'
SENDER_DOMAIN = "triunfoseguros"  # The domain to filter emails from
DATABASE_FILE = "data.db"
global_templates = {}
SELECT_CONTENT = "SELECT content FROM fetched_content WHERE url = ?"
INSERT = (
    "INSERT OR REPLACE INTO fetched_content (url, content, fetch_time) VALUES (?, ?, ?)"
)
REGEX_PDFURL = r"https://www.triunfonet.com.ar/gauswebtriunfo/servlet/(\w+)\?"

def is_valid_url(url):
    """Checks if a string is a potentially valid URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def create_cache_table():
    """Creates the cache table in SQLite if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fetched_content (
            url TEXT PRIMARY KEY,
            content BLOB,
            fetch_time REAL
        )
    """
    )
    conn.commit()
    conn.close()


def get_cached_content(url):
    """Retrieves cached content for a URL if it exists."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    global global_templates
    cursor.execute(SELECT_CONTENT, (url,))
    result = cursor.fetchone()
    if url.startswith("https://l.triunfonet.com.ar/"):
        conn.close()
        return json.loads(result[0].decode()) if result else []
    if result is None:
        return None
    # ----
    # If url is a PDF
    # content is a diff of template
    content = result[0]
    r = re.search(REGEX_PDFURL, url)
    if r:
        urltype = r[1]
        template = global_templates.get(urltype)
        if not template:
            # See if we have a template in db
            cursor.execute(SELECT_CONTENT, (urltype,))
            result = cursor.fetchone()
            if not result:
                # We don't have this template, how? We end this
                conn.close()
                raise ValueError("Invalid value of URL, we don't have a template:", url)
            conn.close()
            template = result[0]
            # save it in memory
            global_templates[urltype] = template
        # patch the template with the content
        patched = bsdiff4.patch(template, content)
        # compress PDF streams
        p = fitz.open(stream=patched, filetype="pdf")
        compressed = p.write( garbage=4,           # Perform garbage collection for maximum cleanup
                deflate=True,        # Use compression for streams (images etc.)
                clean=True,          # Clean up unused objects
                linear=True          # Create a linearized (web-optimized) PDF
                )
        p.close()
        return compressed
    else:
        # unrecognized url, return nothing
        conn.close()
        return None


def cache_content(url, content):
    """Caches the fetched content for a URL."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    global global_templates
    decompressed = None
    if url.startswith("https://l.triunfonet.com.ar/"):
        cursor.execute(INSERT, (url, json.dumps(content).encode(), time.time()))
        conn.commit()
        conn.close()
        return
    # ---
    # If the url is as PDF
    r = re.search(REGEX_PDFURL, url)
    if r:
        urltype = r[1]
        template = global_templates.get(urltype)
        if not template:
            # See if we have a template in db
            cursor.execute(SELECT_CONTENT, (urltype,))
            result = cursor.fetchone()
            if not result:
                # we don't have this template, we save it
                # decompress PDF streams (not images nor fonts)
                p = fitz.open(stream=content, filetype="pdf")
                decompressed = p.write(expand=1, deflate_images=True, deflate_fonts=True)
                p.close()
                cursor.execute(INSERT, (urltype, decompressed, time.time()))
                res = decompressed
            else:
                res = result[0]
            # we save it in memory
            global_templates[urltype] = res
            template = res
        if not decompressed:
            # decompress PDF streams (not images nor fonts)
            p = fitz.open(stream=content, filetype="pdf")
            decompressed = p.write(expand=1, deflate_images=True, deflate_fonts=True)
            p.close()
        content = decompressed
        # diff the template with the content
        d = bsdiff4.diff(template, content)
        # save it
        cursor.execute(INSERT, (url, d, time.time()))
        conn.commit()
        conn.close()
    # else: we don't save other kinds of url


def fetch_and_filter_urls(session, url_to_fetch, find_urls=True):
    """
    Fetches the content of a given URL using the provided session,
    checks the cache, and caches successful responses.

    Args:
        session (requests.Session): The requests session object for connection pooling.
        url_to_fetch (str): The URL to retrieve content from.

    Returns:
        list: A list of valid URLs found in the body of the fetched content,
              or None if an error occurred during fetching.
    """
    found_urls = []
    content = get_cached_content(url_to_fetch)
    if content:
        print(f"Using cached content for '{url_to_fetch}'")
        if find_urls:
            found_urls = content
    else:
        try:
            start_time = time.time()
            response = session.get(url_to_fetch)
            response.raise_for_status()  # Raise an exception for bad status codes
            content = response.content
            end_time = time.time()
            print(
                f"Fetched '{url_to_fetch}' in {end_time - start_time:.2f} seconds and cached."
            )
            if find_urls:
                # we save a JSON array of PDF URLs
                found_urls = find_urls_in_text_javascript(content.decode())
                cache_content(url_to_fetch, found_urls)
            else:
                # PDF
                cache_content(url_to_fetch, content)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching URL '{url_to_fetch}': {e}")
            return None
    for url in found_urls:
        # if we have a list of PDF URLs, we download and store them in cache
        fetch_and_filter_urls(session, url, find_urls=False)


def find_urls_in_text(text):
    """Finds potential URLs within a text string using regex."""
    url_pattern = re.compile(
        r"https?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    )
    potential_urls = url_pattern.findall(text)
    valid_urls = [url for url in potential_urls if is_valid_url(url)]
    return valid_urls


def find_urls_in_text_javascript(text):
    """Finds javascript:self.abre(*) URLs within a text string using regex."""
    url_pattern = re.compile(r"javascript:self.abre\('(.+)'\)")
    potential_urls = url_pattern.findall(text)
    print(potential_urls)
    valid_urls = [url for url in potential_urls if is_valid_url(url)]
    return valid_urls


def fetch_and_scan_emails():
    """Connects to the IMAP server, fetches emails from the specified sender,
    scans the body for URLs, and prints them to the console."""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_ADDRESS, PASSWORD)
        mail.select(MAILBOX)
        session = requests.Session()
        create_cache_table()

        status, email_ids = mail.search(
            None, f'FROM "{SENDER_DOMAIN}"', "UNKEYWORD", "PROCESSED"
        )
        if status == "OK":
            for email_id in email_ids[0].split():
                body = None
                status, msg_data = mail.fetch(email_id, "(RFC822)")
                if status == "OK":
                    msg = email.message_from_bytes(msg_data[0][1])
                    if msg.is_multipart():
                        for part in msg.walk():
                            ctype = part.get_content_type()
                            cdispo = str(part.get("Content-Disposition"))

                            # Look for plain text parts and ignore attachments/HTML
                            if ctype == "text/plain" and "attachment" not in cdispo:
                                body = part.get_payload(decode=True).decode(
                                    errors="ignore"
                                )
                    else:
                        body = msg.get_payload(decode=True).decode(errors="ignore")
                    if body:
                        urls = find_urls_in_text(body)
                        if urls:
                            print(
                                f"--- URLs found in email ID {email_id.decode()} from {msg['From']} ---"
                            )
                            for url in urls:
                                if url.startswith("https://l.triunfonet.com.ar"):
                                    fetch_and_filter_urls(session, url)
                            print("-" * 60)
                        mail.store(email_id, "+FLAGS", ("PROCESSED",))
                else:
                    print(f"Error fetching email {email_id.decode()}: {msg_data}")
        else:
            print(f"Error searching emails: {status}")

        mail.logout()
        session.close()

    except Exception as e:
        # print(f"An error occurred: {e}")
        raise


def extract_files(dest_folder="extracted_pdfs"):
    """Extract cached PDFs to folder"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT url FROM fetched_content WHERE url LIKE 'https://www.triunfonet.com.ar/gauswebtriunfo/servlet/%' order by rowid"
    )
    number = 0

    while True:
        result = cursor.fetchone()
        if result is None:
            break
        number += 1
        url = result[0]
        doc = None
        content = get_cached_content(url)
        if not content:
            continue
        doc = fitz.open(stream=content, filetype="pdf")
        if "hpoliza" in url:
            folder = "pólizas"
            # name
            page = doc[0]
            h = page.rect.height
            num_fac = page.get_text("text", clip=(114, h - 668, 182, h - 654)).strip()
            patente = page.get_text("text", clip=(439, h - 510, 501, h - 491)).strip()
            suplemento = page.get_text(
                "text", clip=(170, h - 631, 203, h - 619)
            ).strip()
            fecha = re.findall(
                r"\d+", page.get_text("text", clip=(133, h - 575, 210, h - 563))
            )
            fecha.reverse()
            name = "_".join(("-".join(fecha), num_fac, suplemento, patente))
        elif "tarjetacir" in url:
            folder = "tarjetas_circulación"
            # name
            page = doc[0]
            h = page.rect.height
            patente = page.get_text("text", clip=(112, h - 151, 234, h - 134)).strip()
            fecha = re.findall(
                r"\d+", page.get_text("text", clip=(228, h - 243, 327, h - 224))
            )
            fecha.reverse()
            name = "_".join(("-".join(fecha), patente))
        elif "tarjetaver" in url:
            folder = "tarjetas_verdes"
            # name
            page = doc[0]
            h = page.rect.height
            patente = page.get_text("text", clip=(64, h - 303, 181, h - 291)).strip()
            fecha = re.findall(
                r"\d+", page.get_text("text", clip=(146, h - 354, 223, h - 342))
            )
            fecha.reverse()
            fecha[1] = fecha[1].zfill(2)
            fecha[2] = fecha[2].zfill(2)
            name = "_".join(("-".join(fecha), patente))
        else:
            folder = "otros"
            name = str(number)
        path = Path(dest_folder).joinpath(folder)
        path.mkdir(parents=True, exist_ok=True)

        if doc:
            with open(path.joinpath(name + ".pdf").as_posix(), "wb") as filew:
                filew.write(content)
            try:
                doc.close()
            except:
                pass

    conn.close()


def convertdb():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT url, content FROM fetched_content WHERE url LIKE 'https://www.triunfonet.com.ar/gauswebtriunfo/servlet%' order by rowid"
    )
    number = 0

    results = cursor.fetchall()
    if results is None:
        return
    for result in results:
        number += 1
        url = result[0]
        doc = None
        content = result[1]
        if not content:
            continue
        # ~ try:
            # ~ doc = fitz.open(stream=content, filetype="pdf")
        # ~ except:
            # ~ breakpoint()
        cache_content(url, content)
        # ~ doc.close()
    conn.close()


if __name__ == "__main__":
    if "--extract" in sys.argv:
        extract_files()
    else:
        # ~ fetch_and_scan_emails()
        convertdb()

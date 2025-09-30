import imaplib
import email
import re
from urllib.parse import urlparse
import requests
import sqlite3
import time
from datetime import date
from pathlib import Path
import sys
import json
import bsdiff4
import fitz
import os
import openpyxl
import hashlib 

# --- Configuration ---
# --- You may need to change the config below---
IMAP_SERVER = (
    "imap.gmail.com"  # Replace with your IMAP server address (e.g., imap.gmail.com)
)
EMAIL_ADDRESS = "user"  # Replace with your email address or IMAP user
PASSWORD = "pass"  # Replace with your email password
# The mailbox to check (e.g., "INBOX", "Sent") If spaces in name, use "" i.e.: '"mail box"'
MAILBOX = 'INBOX/Some'

# --- You may not need to change the config below ---
SENDER_DOMAIN = "triunfoseguros"  # The domain to filter emails from
DATABASE_FILE = "data.db"
global_templates = {}
SELECT_CONTENT = "SELECT content FROM fetched_content WHERE url = ?"
INSERT = (
    "INSERT OR IGNORE INTO fetched_content (url, filename, content, fetch_time) VALUES (?, ?, ?, ?)"
)
REGEX_PDFURL = r"https://www.triunfonet.com.ar/gauswebtriunfo/servlet/(\w+)\?"

BOLD = openpyxl.styles.Font(bold=True)
NORMAL = openpyxl.styles.Font(bold=False)
RIGTH = openpyxl.styles.Alignment(horizontal='right', textRotation=30)
LEFT = openpyxl.styles.Alignment(horizontal='left', textRotation=30)
CENTER = openpyxl.styles.Alignment(horizontal='center', textRotation=30)

DATE_FILTER_SINCE = None #'01-Aug-2025'

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
        conn.close()
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
        cursor.execute(INSERT, (url, None, json.dumps(content).encode(), time.time()))
        conn.commit()
        conn.close()
        return
    # ---
    # If the url is as PDF
    r = re.search(REGEX_PDFURL, url)
    if r:
        urltype = r[1]
        name = None
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
                if '/hpoliza' in url:
                    name = get_name_poliza(p)[1]
                p.close()
                cursor.execute(INSERT, (urltype, None, decompressed, time.time()))
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
            if '/hpoliza' in url:
                name = get_name_poliza(p)[1]
                url += name
            p.close()
        content = decompressed
        # diff the template with the content
        d = bsdiff4.diff(template, content)
        # save it
        cursor.execute(INSERT, (url, name, d, time.time()))
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
        find_urls (bool): Parse html content for URLs. If False, only cache content.

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
                f"Fetched '{url_to_fetch}' in {end_time - start_time:.2f} seconds."
            )
            if find_urls:
                # we save a JSON array of PDF URLs
                found_urls = find_urls_in_text_javascript(content.decode())
                if found_urls:
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
        r"https?://[^\s<>\[\]\"\']+"
    )
    potential_urls = url_pattern.findall(text)
    valid_urls = [url for url in potential_urls if is_valid_url(url)]
    return valid_urls


def find_urls_in_text_javascript(text):
    """Finds javascript:self.abre(*) URLs within a text string using regex."""
    url_pattern = re.compile(r"javascript:self.abre\('(.+)'\)")
    potential_urls = url_pattern.findall(text)
    valid_urls = [url for url in potential_urls if is_valid_url(url)]
    return valid_urls


def fetch_and_scan_emails():
    """Connects to the IMAP server, fetches emails from the specified sender,
    scans the body for URLs, and prints them to the console."""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_ADDRESS, PASSWORD)
        result = mail.select(MAILBOX)
        if result[0] != 'OK':
            mail.logout()
            print (result[1])
            return -1
        session = requests.Session()
        create_cache_table()

        search_criteria = [f'FROM "{SENDER_DOMAIN}"', "UNKEYWORD", "PROCESSED"]

        # Add SINCE criteria if a date is configured
        if DATE_FILTER_SINCE:
            search_criteria.append("SINCE")
            search_criteria.append(DATE_FILTER_SINCE)

        status, email_ids = mail.search(
            None, *search_criteria
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

def get_name_poliza(doc, excel=False):
    """Parses a fitz PDF doc and extracts some important info"""
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
        r"\d+", page.get_text("text", clip=((148.67, 250.00, 215.33, 270.67)))
    )
    name = "_".join([f'{fecha[2]}-{fecha[1]}@{fecha[5]}-{fecha[4]}',
                     num_fac, suplemento, patente])
    if not excel:
        return folder, name
        
    # excel
    num_fac = safefloat(num_fac, thousands_sep=".")
    
    premio = page.get_text(
        "text", clip=(134, 520, 190, 534)
    ).strip()
    premio = safefloat(premio, thousands_sep=".")
    
    prima = page.get_text(
        "text", clip=(122, 415, 207, 424)
    ).strip()
    prima = safefloat(prima, thousands_sep=".")
    
    iva = page.get_text(
        "text", clip=(122, 424, 207, 434)
    ).strip()
    iva = safefloat(iva, thousands_sep=".")
    
    af = page.get_text(
        "text", clip=(162, 434, 204, 441)
    ).strip()
    af = safefloat(af, thousands_sep=".")
    
    iva_af = page.get_text(
        "text", clip=(122, 444, 207, 450)
    ).strip()
    iva_af = safefloat(iva_af, thousands_sep=".")

    sellos = page.get_text(
        "text", clip=(122, 451, 206, 460)
    ).strip()
    sellos = safefloat(sellos, thousands_sep=".")
    
    otros_imp = page.get_text(
        "text", clip=(122, 469, 206, 478)
    ).strip()
    otros_imp = safefloat(otros_imp, thousands_sep=".")
    
    otros_grv = page.get_text(
        "text", clip=(122, 479, 206, 487)
    ).strip()
    otros_grv = safefloat(otros_grv, thousands_sep=".")
    
    cuotas_soc = page.get_text(
        "text", clip=(122, 487, 206, 496)
    ).strip()
    cuotas_soc = safefloat(cuotas_soc, thousands_sep=".")
    
    return (fecha, num_fac, suplemento, patente, premio, prima, iva, af,
            iva_af, sellos, otros_imp, otros_grv, cuotas_soc)
            
def file_save(name, content):
    if os.path.exists(name):
        return
    with open(name, "wb") as filew:
        filew.write(content)

def extract_file(url, return_bytes=False, excel=False):
    """Save a file from a url or return the bytes of the file or return Excel data"""
    doc = None
    content = get_cached_content(url)
    if not content:
        print("No content at URL:" + url)
        return None, None
    doc = fitz.open(stream=content, filetype="pdf")
    if not doc:
        print("Error on fitz.open at URL:" + url)
        return None, None
    if "hpoliza" in url:
        if not excel:
            folder, name = get_name_poliza(doc)
        else:
            datos = get_name_poliza(doc, excel)
            doc.close()
            return datos
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
    path = Path("extracted_pdfs").joinpath(folder)
    if not return_bytes:
        path.mkdir(parents=True, exist_ok=True)
    #if doc: (doc is not None here)
    fullname = path.joinpath(name + ".pdf").as_posix()
    if not return_bytes:
        file_save(fullname, content)
    else:
        return name, content
    doc.close()


def extract_files():
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
        extract_file(result[0])
    conn.close()


def ingest(files):
    "Add old pólizas to db"
    for file in files:
        if os.path.isdir(file):
            pdf_files = [f for f in Path(file).rglob('*') if f.suffix.lower() == '.pdf']
        else:
            pdf_files = [Path(file)]
        for f in pdf_files:
            print(f)
            with f.open("rb") as of:
                content = of.read()
                cache_content("https://www.triunfonet.com.ar/gauswebtriunfo/servlet/hpolizapd?--", content)

def cell2(ws, row=None, column=None, value=None, number_format=None, fill=None, font=BOLD, 
          align=None, col_width=None):
    cell = ws.cell(row=row, column=column, value=value)
    if fill:
        cell.fill = fill
    if font:
        cell.font = font
    if number_format:
        cell.number_format = number_format
    if align:
        cell.alignment = align
    if col_width:
        ws.column_dimensions[cell.column_letter].width = col_width
    return cell

def sort_key_excel(item):
    date_parts = item[0]
    year = int(date_parts[2])
    month = int(date_parts[1])
    day = int(date_parts[0])
    return (year, month, day)

def safefloat(n, thousands_sep=","):
    n = n.strip()
    n2=""
    if not n:
        return ''
    n1 = re.split(r'\s+',n)
    for i in n1:
        if i[-1].isnumeric():
            n2 = i
            break
    if not n2:
        return ''
    # ~ try:
    if thousands_sep==".":
        r = float(n2.replace(thousands_sep,"").replace(",","."))
    elif thousands_sep==",":
        r = float(n2.replace(thousands_sep,""))
    else:
        r = ""
    # ~ except:
        # ~ r = ""
    return r

def excel():
    """Generate an Excel sheet from the info in database"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT url FROM fetched_content WHERE url LIKE 'https://www.triunfonet.com.ar/gauswebtriunfo/servlet/hpolizapd%' order by rowid"
    )
    wb = openpyxl.Workbook()
    ws = wb.active
    row = 0
    datos = []
    while True:
        result = cursor.fetchone()
        if result is None:
            break
        dato = extract_file(result[0], excel=True)
        datos.append(dato)
        
    sorted_data = sorted(datos, key=sort_key_excel)
     # ~ return (fecha, num_fac, suplemento, patente, premio, prima, iva, iva_af),
            # ~ af, sellos, otros_imp, otros_grv, cuotas_soc)
    
    row = 1
    cell2(ws, row, 1, 'Fecha desde', font=BOLD, align=LEFT)
    cell2(ws, row, 2, 'Fecha hasta', font=BOLD, align=LEFT)
    cell2(ws, row, 3, 'Número póliza', font=BOLD, align=LEFT)
    cell2(ws, row, 4, 'Suplemento', font=BOLD, align=LEFT)
    cell2(ws, row, 5, 'Patente', font=BOLD, align=LEFT)
    cell2(ws, row, 6, 'Total pagado', font=BOLD, align=LEFT)
    cell2(ws, row, 7, 'Prima', font=BOLD, align=LEFT)
    cell2(ws, row, 8, 'IVA', font=BOLD, align=LEFT)
    cell2(ws, row, 9, 'Adic. Financiero', font=BOLD, align=LEFT)
    cell2(ws, row, 10, 'IVA Adic. Financ.', font=BOLD, align=LEFT)
    cell2(ws, row, 11, 'Sellos', font=BOLD, align=LEFT)
    cell2(ws, row, 12, 'Otros Imp.', font=BOLD, align=LEFT)
    cell2(ws, row, 13, 'Otros Grav.', font=BOLD, align=LEFT)
    cell2(ws, row, 14, 'Cuotas sociales', font=BOLD, align=LEFT)
   
    for dato in sorted_data:
        # ~ print (dato)
        # ~ breakpoint()
        row += 1
        fecha_desde = dato[0][:3]
        fecha_desde.reverse()
        fecha_desde = "-".join(fecha_desde)
        
        fecha_hasta = dato[0][3:]
        fecha_hasta.reverse()
        fecha_hasta = "-".join(fecha_hasta)
        
        cell2(ws, row, 1, date.fromisoformat(fecha_desde), number_format='DD/MM/YYYY', col_width=11)
        cell2(ws, row, 2, date.fromisoformat(fecha_hasta), number_format='DD/MM/YYYY', col_width=11)
        cell2(ws, row, 3, dato[1], number_format = '#,##', col_width=11) #num
        cell2(ws, row, 4, int(dato[2]) if dato[2] else None, col_width=4) #sup
        cell2(ws, row, 5, dato[3], col_width=11) #pat
        cell2(ws, row, 6, dato[4], number_format = '#,##0.00', col_width=11) #prem
        cell2(ws, row, 7, dato[5], number_format = '#,##0.00', col_width=11) #prima
        cell2(ws, row, 8, dato[6], number_format = '#,##0.00', col_width=11) #iva
        cell2(ws, row, 9, dato[7], number_format = '#,##0.00', col_width=11) #af
        cell2(ws, row, 10, dato[8], number_format = '#,##0.00', col_width=11) #ivaaf
        cell2(ws, row, 11, dato[9], number_format = '#,##0.00', col_width=11) #sellos
        cell2(ws, row, 12, dato[10], number_format = '#,##0.00', col_width=11) #oi
        cell2(ws, row, 13, dato[11], number_format = '#,##0.00', col_width=11) #og
        cell2(ws, row, 14, dato[12], number_format = '#,##0.00', col_width=11) #cuotas
    ws.freeze_panes = 'A2'
    wb.save("datos.xlsx") 
    conn.close()

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--extract":
        extract_files()
    elif len(sys.argv) > 1 and sys.argv[1] == "--ingest":
        ingest(sys.argv[2:])
    elif len(sys.argv) > 1 and sys.argv[1] == "--excel":
        excel()
    else:
        fetch_and_scan_emails()

if __name__ == "__main__":
    main()

import os
import base64
import json
import sqlite3
import secrets
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, abort, jsonify, flash, session, make_response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(DEFAULT_DATA_DIR, "quotes.db"))

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)
UPLOADS_DIR = os.path.join(STATIC_DIR, "uploads")
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(16))

try:
    from weasyprint import HTML
except Exception:
    HTML = None


ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "DJs2025!")

DEFAULT_LABOR_RATE = 150.00
DEFAULT_TAX_RATE = 7.25
DEFAULT_SERVICE_FEE = 0.00

JOB_PRESETS = {
    "oil_filter_change": {
        "name": "Oil & Filter Change",
        "jobs": [
            {
                "desc": "Oil & Filter Change",
                "labor_hours": 0.50,
                "notes": "Includes oil and filter replacement. Additional oil, specialty filters, skid plate removal, or cartridge housing issues may change final price.",
                "parts": [
                    {"part_desc": "Full Synthetic Engine Oil", "qty": 1, "oem": 65.00, "quality": 45.00, "economy": 35.00},
                    {"part_desc": "Oil Filter", "qty": 1, "oem": 18.00, "quality": 12.00, "economy": 9.00},
                ],
            }
        ],
    },
    "rotate_balance": {
        "name": "Tire Rotation",
        "jobs": [
            {
                "desc": "Tire Rotation",
                "labor_hours": 0.40,
                "notes": "Rotation only. Tire balance, road force, TPMS service, or damaged lug nuts are additional if needed.",
                "parts": [],
            }
        ],
    },
    "battery_replace": {
        "name": "Battery Replacement",
        "jobs": [
            {
                "desc": "Battery Replacement",
                "labor_hours": 0.40,
                "notes": "Includes battery install and terminal cleaning. Registration/programming extra when required.",
                "parts": [
                    {"part_desc": "Battery", "qty": 1, "oem": 245.00, "quality": 185.00, "economy": 150.00},
                ],
            }
        ],
    },
    "front_brakes": {
        "name": "Front Brake Pads & Rotors",
        "jobs": [
            {
                "desc": "Front Brake Pads & Rotors",
                "labor_hours": 1.50,
                "notes": "Price may change if calipers, brackets, seized hardware, or brake hoses are needed.",
                "parts": [
                    {"part_desc": "Front Brake Pads", "qty": 1, "oem": 145.00, "quality": 95.00, "economy": 70.00},
                    {"part_desc": "Front Brake Rotors", "qty": 2, "oem": 120.00, "quality": 85.00, "economy": 65.00},
                ],
            }
        ],
    },
    "spark_plugs_4cyl": {
        "name": "Spark Plug Replacement (4 Cylinder)",
        "jobs": [
            {
                "desc": "Spark Plug Replacement",
                "labor_hours": 1.00,
                "notes": "Pricing shown for an accessible 4-cylinder layout. Intake removal, plenum gaskets, or broken boots are extra if needed.",
                "parts": [
                    {"part_desc": "Spark Plugs", "qty": 4, "oem": 24.00, "quality": 16.00, "economy": 10.00},
                ],
            }
        ],
    },
}


def slugify(value):
    cleaned = ''.join(ch.lower() if ch.isalnum() else '_' for ch in (value or '').strip())
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    return cleaned.strip('_') or 'preset'


def build_quote_slug_base(customer_name, vehicle=''):
    customer_name = (customer_name or '').strip()
    vehicle = (vehicle or '').strip()
    name_parts = [part for part in customer_name.replace(',', ' ').split() if part]
    last_name = slugify(name_parts[-1]) if name_parts else 'customer'
    vehicle_words = [part for part in vehicle.replace('-', ' ').split() if part]
    vehicle_hint = slugify(' '.join(vehicle_words[-2:])) if vehicle_words else ''
    if vehicle_hint and vehicle_hint != 'preset':
        return f"{last_name}-{vehicle_hint}"
    return last_name


def generate_quote_public_slug(conn, customer_name, vehicle=''):
    base = build_quote_slug_base(customer_name, vehicle)
    for _ in range(50):
        suffix = secrets.token_hex(2)
        slug = f"{base}-{suffix}"
        existing = conn.execute("SELECT id FROM quotes WHERE public_slug = ?", (slug,)).fetchone()
        if not existing:
            return slug
    return f"{base}-{secrets.token_hex(4)}"


def get_quote_public_key(quote):
    if not quote:
        return ''
    if isinstance(quote, sqlite3.Row):
        slug = quote['public_slug'] if 'public_slug' in quote.keys() else ''
        token = quote['quote_token'] if 'quote_token' in quote.keys() else ''
    else:
        slug = quote.get('public_slug', '')
        token = quote.get('quote_token', '')
    return (slug or token or '').strip()


def normalize_preset_jobs(jobs):
    normalized_jobs = []
    if not isinstance(jobs, list):
        return normalized_jobs

    for job in jobs:
        if not isinstance(job, dict):
            continue
        normalized_jobs.append({
            "desc": (job.get("desc") or "").strip(),
            "labor_hours": safe_float(job.get("labor_hours", 0), 0),
            "notes": (job.get("notes") or "").strip(),
            "parts": normalize_job_parts(job),
        })
    return normalized_jobs





def normalize_phone_number(value):
    digits = ''.join(ch for ch in str(value or '') if ch.isdigit())
    if not digits:
        return ''
    if len(digits) == 11 and digits.startswith('1'):
        return f'+{digits}'
    if len(digits) == 10:
        return f'+1{digits}'
    if str(value).strip().startswith('+'):
        return str(value).strip()
    return f'+{digits}'


def display_phone_number(value):
    digits = ''.join(ch for ch in str(value or '') if ch.isdigit())
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f'({digits[:3]}) {digits[3:6]}-{digits[6:]}'
    return str(value or '').strip()


def get_public_base_url():
    configured = (os.environ.get('PUBLIC_BASE_URL') or '').strip()
    if configured:
        return configured.rstrip('/')
    try:
        return request.url_root.rstrip('/')
    except RuntimeError:
        return ''


def build_quote_url_external(quote_or_key):
    base = get_public_base_url()
    if isinstance(quote_or_key, (sqlite3.Row, dict)):
        key = get_quote_public_key(quote_or_key)
    else:
        key = str(quote_or_key or '').strip()
    return f"{base}/quote/{key}" if base else f"/quote/{key}"


def build_copy_text_message(quote):
    customer_name = (quote['customer_name'] or '').strip()
    first_name = customer_name.split()[0] if customer_name else 'there'
    vehicle = (quote['vehicle'] or 'your vehicle').strip()
    quote_url = build_quote_url_external(quote)

    lines = [
        f"Hey {first_name}, this is DJ with DJ's Mobile Mechanic.",
        '',
        f"Here is the quote for {vehicle}:",
        quote_url,
        '',
        "You can review it and approve it right from the link.",
        "Let me know if you have any questions.",
    ]
    return '\n'.join(lines)


def get_email_settings():
    return {
        "host": (os.environ.get("SMTP_HOST") or "").strip(),
        "port": safe_int(os.environ.get("SMTP_PORT"), 587),
        "username": (os.environ.get("SMTP_USERNAME") or "").strip(),
        "password": (os.environ.get("SMTP_PASSWORD") or "").strip(),
        "from_email": (os.environ.get("MAIL_FROM") or os.environ.get("SMTP_USERNAME") or "").strip(),
        "from_name": (os.environ.get("MAIL_FROM_NAME") or "DJ's Mobile Mechanic").strip(),
        "use_tls": safe_bool(os.environ.get("SMTP_USE_TLS"), True),
        "reply_to": (os.environ.get("MAIL_REPLY_TO") or os.environ.get("MAIL_FROM") or os.environ.get("SMTP_USERNAME") or "").strip(),
    }


def email_settings_ready():
    settings = get_email_settings()
    return bool(settings["host"] and settings["port"] and settings["username"] and settings["password"] and settings["from_email"])


def get_quote_email_subject(quote):
    customer_name = (quote["customer_name"] or "Customer").strip()
    vehicle = (quote["vehicle"] or "your vehicle").strip()
    return f"Your Quote from DJ's Mobile Mechanic - {customer_name} - {vehicle}"


def build_quote_email_body(quote):
    customer_name = (quote["customer_name"] or "").strip()
    first_name = customer_name.split()[0] if customer_name else "there"
    vehicle = (quote["vehicle"] or "your vehicle").strip()
    quote_url = build_quote_url_external(quote)

    return (
        f"Hi {first_name},\n\n"
        f"Attached is your quote from DJ's Mobile Mechanic for {vehicle}.\n\n"
        f"You can also review and approve it online here:\n{quote_url}\n\n"
        "If you have any questions, just reply to this email or text/call me.\n\n"
        "Thank you,\n"
        "DJ's Mobile Mechanic"
    )


def get_quote_for_any_token(conn, token):
    return conn.execute(
        "SELECT * FROM quotes WHERE quote_token = ? OR public_slug = ?",
        (token, token),
    ).fetchone()


def build_quote_template_context(quote_row):
    quote = dict(quote_row)
    payload = json.loads(quote["payload_json"] or "{}")
    jobs = payload.get("jobs", [])
    jobs = [{**job, "parts": normalize_job_parts(job)} for job in jobs]
    _approved_jobs, approved_map = parse_approved_map(quote.get("approved_json"))
    inspection = load_inspection(quote)
    return {
        "quote": quote,
        "jobs": jobs,
        "approved_map": approved_map,
        "inspection": inspection,
    }


def make_safe_attachment_name(quote):
    customer = slugify(quote["customer_name"] or "customer")
    vehicle = slugify(quote["vehicle"] or "vehicle")
    return f"quote-{customer}-{vehicle}.pdf"


def render_quote_pdf_bytes(quote_row):
    if HTML is None:
        raise RuntimeError("WeasyPrint is not installed. Add it to requirements.txt and redeploy.")
    html = render_template("quote.html", **build_quote_template_context(quote_row))
    base_url = get_public_base_url() or request.url_root.rstrip("/")
    return HTML(string=html, base_url=base_url).write_pdf()


def send_quote_email_message(quote_row, pdf_bytes):
    settings = get_email_settings()
    recipient = (quote_row["customer_email"] or "").strip()
    if not recipient:
        raise ValueError("This quote does not have a customer email address.")
    if not email_settings_ready():
        raise RuntimeError("Email settings are incomplete. Add SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, and MAIL_FROM in Render.")

    msg = EmailMessage()
    from_name = settings["from_name"]
    from_email = settings["from_email"]
    msg["Subject"] = get_quote_email_subject(quote_row)
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"] = recipient
    if settings["reply_to"]:
        msg["Reply-To"] = settings["reply_to"]
    msg.set_content(build_quote_email_body(quote_row))

    attachment_name = make_safe_attachment_name(quote_row)
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=attachment_name)

    with smtplib.SMTP(settings["host"], settings["port"]) as server:
        server.ehlo()
        if settings["use_tls"]:
            server.starttls()
            server.ehlo()
        server.login(settings["username"], settings["password"])
        server.send_message(msg)



def build_logo_data_uri():
    logo_path = os.path.join(STATIC_DIR, "logo.png")
    if not os.path.exists(logo_path):
        return ""
    try:
        with open(logo_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        return f"data:image/png;base64,{encoded}"
    except Exception:
        return ""


@app.context_processor
def inject_brand_assets():
    return {"logo_data_uri": build_logo_data_uri()}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def safe_float(value, default=0.0):
    try:
        return float(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        return default


def safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    return default


def safe_int(value, default=0):
    try:
        return int(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        return default


app.permanent_session_lifetime = timedelta(minutes=max(safe_int(os.environ.get("ADMIN_SESSION_MINUTES"), 120), 15))


def admin_login_url():
    next_url = request.full_path if request.query_string else request.path
    return url_for("login", next=next_url)


def login_admin_session():
    session.clear()
    session.permanent = True
    session["admin_logged_in"] = True
    session["admin_last_seen"] = datetime.now().isoformat()


def clear_admin_session():
    session.pop("admin_logged_in", None)
    session.pop("admin_last_seen", None)


def admin_session_active():
    if not session.get("admin_logged_in"):
        return False

    raw_last_seen = session.get("admin_last_seen")
    if not raw_last_seen:
        clear_admin_session()
        return False

    try:
        last_seen = datetime.fromisoformat(raw_last_seen)
    except (TypeError, ValueError):
        clear_admin_session()
        return False

    if datetime.now() - last_seen > app.permanent_session_lifetime:
        clear_admin_session()
        return False

    session["admin_last_seen"] = datetime.now().isoformat()
    session.permanent = True
    return True


def admin_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if not admin_session_active():
            flash("Please log in to access the admin area.")
            return redirect(admin_login_url())
        return view_func(*args, **kwargs)
    return wrapped_view


def table_columns(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row["name"] for row in cur.fetchall()]


def add_column_if_missing(conn, table_name, column_name, column_def):
    cols = table_columns(conn, table_name)
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS vehicles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        vehicle_text TEXT NOT NULL,
        vin TEXT,
        created_at TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_token TEXT UNIQUE,
        public_slug TEXT UNIQUE,
        created_at TEXT,
        customer_id INTEGER,
        vehicle_id INTEGER,
        customer_name TEXT,
        customer_phone TEXT,
        customer_email TEXT,
        vehicle TEXT,
        vin TEXT,
        labor_rate REAL,
        tax_rate REAL,
        service_fee REAL,
        payload_json TEXT,
        approved_json TEXT,
        signature_data TEXT,
        signed_name TEXT,
        signed_at TEXT,
        status TEXT DEFAULT 'quote',
        FOREIGN KEY (customer_id) REFERENCES customers(id),
        FOREIGN KEY (vehicle_id) REFERENCES vehicles(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_number TEXT UNIQUE,
        quote_id INTEGER,
        created_at TEXT,
        total REAL,
        payment_status TEXT DEFAULT 'unpaid',
        payment_method TEXT,
        paid_at TEXT,
        FOREIGN KEY (quote_id) REFERENCES quotes(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS saved_job_presets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        preset_key TEXT UNIQUE,
        preset_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT,
        updated_at TEXT,
        is_system INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS request_quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        customer_name TEXT,
        customer_phone TEXT,
        customer_email TEXT,
        vehicle TEXT,
        vin TEXT,
        requested_service TEXT,
        concern_details TEXT,
        preferred_schedule TEXT,
        status TEXT DEFAULT 'new',
        source TEXT DEFAULT 'website'
    )
    """)

    conn.commit()

    add_column_if_missing(conn, "quotes", "quote_token", "TEXT")
    add_column_if_missing(conn, "quotes", "public_slug", "TEXT")
    add_column_if_missing(conn, "quotes", "customer_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "vehicle_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "approved_json", "TEXT")
    add_column_if_missing(conn, "quotes", "signature_data", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_name", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_at", "TEXT")
    add_column_if_missing(conn, "quotes", "status", "TEXT DEFAULT 'quote'")
    add_column_if_missing(conn, "quotes", "inspection_json", "TEXT")
    add_column_if_missing(conn, "quotes", "parts_tracking_json", "TEXT")
    add_column_if_missing(conn, "quotes", "admin_status", "TEXT DEFAULT 'active'")
    add_column_if_missing(conn, "quotes", "waiting_since", "TEXT")
    add_column_if_missing(conn, "quotes", "archived_at", "TEXT")

    add_column_if_missing(conn, "invoices", "payment_status", "TEXT DEFAULT 'unpaid'")
    add_column_if_missing(conn, "invoices", "payment_method", "TEXT")
    add_column_if_missing(conn, "invoices", "paid_at", "TEXT")

    add_column_if_missing(conn, "request_quotes", "created_at", "TEXT")
    add_column_if_missing(conn, "request_quotes", "customer_name", "TEXT")
    add_column_if_missing(conn, "request_quotes", "customer_phone", "TEXT")
    add_column_if_missing(conn, "request_quotes", "customer_email", "TEXT")
    add_column_if_missing(conn, "request_quotes", "vehicle", "TEXT")
    add_column_if_missing(conn, "request_quotes", "vin", "TEXT")
    add_column_if_missing(conn, "request_quotes", "requested_service", "TEXT")
    add_column_if_missing(conn, "request_quotes", "concern_details", "TEXT")
    add_column_if_missing(conn, "request_quotes", "preferred_schedule", "TEXT")
    add_column_if_missing(conn, "request_quotes", "status", "TEXT DEFAULT 'new'")
    add_column_if_missing(conn, "request_quotes", "source", "TEXT DEFAULT 'website'")

    conn.commit()
    conn.close()


init_db()


def generate_token():
    return secrets.token_urlsafe(8)


def generate_invoice_number():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) AS max_id FROM invoices")
    row = cur.fetchone()
    next_id = (row["max_id"] or 0) + 1
    conn.close()
    return f"INV-{1000 + next_id}"


def find_or_create_customer(conn, name, phone, email):
    name = (name or "").strip()
    phone = (phone or "").strip()
    email = (email or "").strip().lower()

    if not name:
        return None

    cur = conn.cursor()

    if phone:
        cur.execute("SELECT * FROM customers WHERE phone = ?", (phone,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE customers SET name = ?, email = ? WHERE id = ?",
                (name, email, existing["id"]),
            )
            conn.commit()
            return existing["id"]

    if email:
        cur.execute("SELECT * FROM customers WHERE email = ?", (email,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE customers SET name = ?, phone = ? WHERE id = ?",
                (name, phone, existing["id"]),
            )
            conn.commit()
            return existing["id"]

    cur.execute(
        "INSERT INTO customers (name, phone, email, created_at) VALUES (?, ?, ?, ?)",
        (name, phone, email, datetime.now().isoformat()),
    )
    conn.commit()
    return cur.lastrowid


def find_or_create_vehicle(conn, customer_id, vehicle_text, vin):
    vehicle_text = (vehicle_text or "").strip()
    vin = (vin or "").strip().upper()

    if not vehicle_text or not customer_id:
        return None

    cur = conn.cursor()

    if vin:
        cur.execute(
            "SELECT * FROM vehicles WHERE customer_id = ? AND vin = ?",
            (customer_id, vin),
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE vehicles SET vehicle_text = ? WHERE id = ?",
                (vehicle_text, existing["id"]),
            )
            conn.commit()
            return existing["id"]

    cur.execute(
        "SELECT * FROM vehicles WHERE customer_id = ? AND vehicle_text = ?",
        (customer_id, vehicle_text),
    )
    existing = cur.fetchone()
    if existing:
        if vin and not existing["vin"]:
            cur.execute("UPDATE vehicles SET vin = ? WHERE id = ?", (vin, existing["id"]))
            conn.commit()
        return existing["id"]

    cur.execute(
        "INSERT INTO vehicles (customer_id, vehicle_text, vin, created_at) VALUES (?, ?, ?, ?)",
        (customer_id, vehicle_text, vin, datetime.now().isoformat()),
    )
    conn.commit()
    return cur.lastrowid


def normalize_job_parts(job):
    if isinstance(job.get("parts"), list):
        normalized = []
        for part in job["parts"]:
            if not isinstance(part, dict):
                continue
            oem = safe_float(part.get("oem", 0))
            quality = safe_float(part.get("quality", 0))
            economy = safe_float(part.get("economy", 0))

            enabled_oem = safe_bool(part.get("enabled_oem"), oem > 0)
            enabled_quality = safe_bool(part.get("enabled_quality"), quality > 0 or (not enabled_oem and economy <= 0))
            enabled_economy = safe_bool(part.get("enabled_economy"), economy > 0)
            selected_tier = sanitize_selected_tier({
                "enabled_oem": enabled_oem,
                "enabled_quality": enabled_quality,
                "enabled_economy": enabled_economy,
            }, part.get("selected_tier"), default_tier="quality")

            normalized.append({
                "part_desc": (part.get("part_desc") or "").strip(),
                "qty": max(safe_float(part.get("qty", 1), 1), 0),
                "oem": oem,
                "quality": quality,
                "economy": economy,
                "list_oem": safe_float(part.get("list_oem", oem)),
                "list_quality": safe_float(part.get("list_quality", quality)),
                "list_economy": safe_float(part.get("list_economy", economy)),
                "oem_part_number": (part.get("oem_part_number") or part.get("part_number_oem") or "").strip(),
                "quality_part_number": (part.get("quality_part_number") or part.get("part_number_quality") or "").strip(),
                "economy_part_number": (part.get("economy_part_number") or part.get("part_number_economy") or "").strip(),
                "source_oem": (part.get("source_oem") or part.get("part_source_oem") or "online_oem").strip(),
                "source_quality": (part.get("source_quality") or part.get("part_source_quality") or "online_oem").strip(),
                "source_economy": (part.get("source_economy") or part.get("part_source_economy") or "online_oem").strip(),
                "buffer_oem": safe_float(part.get("buffer_oem", part.get("part_buffer_oem", 25)), 25),
                "buffer_quality": safe_float(part.get("buffer_quality", part.get("part_buffer_quality", 25)), 25),
                "buffer_economy": safe_float(part.get("buffer_economy", part.get("part_buffer_economy", 25)), 25),
                "flat_markup_oem": safe_float(part.get("flat_markup_oem", part.get("markup_oem", 0)), 0),
                "flat_markup_quality": safe_float(part.get("flat_markup_quality", part.get("markup_quality", 0)), 0),
                "flat_markup_economy": safe_float(part.get("flat_markup_economy", part.get("markup_economy", 0)), 0),
                "markup_oem": safe_float(part.get("markup_oem", part.get("flat_markup_oem", 0)), 0),
                "markup_quality": safe_float(part.get("markup_quality", part.get("flat_markup_quality", 0)), 0),
                "markup_economy": safe_float(part.get("markup_economy", part.get("flat_markup_economy", 0)), 0),
                "enabled_oem": enabled_oem,
                "enabled_quality": enabled_quality,
                "enabled_economy": enabled_economy,
                "selected_tier": selected_tier,
            })
        return normalized

    old_oem = safe_float(job.get("parts_oem", 0))
    old_quality = safe_float(job.get("parts_quality", 0))
    old_economy = safe_float(job.get("parts_economy", 0))
    if old_oem or old_quality or old_economy:
        return [{
            "part_desc": "Parts",
            "qty": 1,
            "oem": old_oem,
            "quality": old_quality,
            "economy": old_economy,
            "list_oem": old_oem,
            "list_quality": old_quality,
            "list_economy": old_economy,
            "oem_part_number": "",
            "quality_part_number": "",
            "economy_part_number": "",
            "source_oem": "online_oem",
            "source_quality": "online_oem",
            "source_economy": "online_oem",
            "buffer_oem": 25,
            "buffer_quality": 25,
            "buffer_economy": 25,
            "flat_markup_oem": 0,
            "flat_markup_quality": 0,
            "flat_markup_economy": 0,
            "markup_oem": 0,
            "markup_quality": 0,
            "markup_economy": 0,
            "enabled_oem": old_oem > 0,
            "enabled_quality": old_quality > 0,
            "enabled_economy": old_economy > 0,
            "selected_tier": sanitize_selected_tier({
                "enabled_oem": old_oem > 0,
                "enabled_quality": old_quality > 0,
                "enabled_economy": old_economy > 0,
            }, "quality", default_tier="quality"),
        }]
    return []


def get_enabled_tiers(part):
    tiers = []
    if part.get("enabled_oem"):
        tiers.append("oem")
    if part.get("enabled_quality"):
        tiers.append("quality")
    if part.get("enabled_economy"):
        tiers.append("economy")
    if not tiers:
        tiers.append("quality")
    return tiers


def sanitize_selected_tier(part, requested_tier=None, default_tier="quality"):
    enabled = get_enabled_tiers(part)
    desired = (requested_tier or default_tier or "quality").strip().lower()
    if desired in enabled:
        return desired
    if default_tier in enabled:
        return default_tier
    return enabled[0]


PARTS_TRACKER_STATUSES = ["need_to_order", "ordered", "received", "installed"]


def normalize_parts_tracker_status(value):
    value = (value or "need_to_order").strip().lower().replace(" ", "_")
    return value if value in PARTS_TRACKER_STATUSES else "need_to_order"


def pretty_tier_name(value):
    tier = (value or "").strip().lower()
    if tier == "oem":
        return "OEM"
    if tier == "quality":
        return "Quality"
    if tier == "economy":
        return "Economy"
    return ""


def parse_parts_tracking_json(parts_tracking_json):
    try:
        data = json.loads(parts_tracking_json or "[]")
    except Exception:
        data = []
    return data if isinstance(data, list) else []


def build_quote_parts_tracker(payload_json, parts_tracking_json=None):
    try:
        payload = json.loads(payload_json or "{}")
    except Exception:
        payload = {}

    existing_items = parse_parts_tracking_json(parts_tracking_json)
    existing_map = {}
    for item in existing_items:
        if isinstance(item, dict) and item.get("source_key"):
            existing_map[item.get("source_key")] = item

    tracker_items = []
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    for job_index, job in enumerate(jobs):
        if not isinstance(job, dict):
            continue
        job_desc = (job.get("desc") or "Service").strip() or "Service"
        for part_index, part in enumerate(normalize_job_parts(job)):
            part_desc = (part.get("part_desc") or "Part").strip() or "Part"
            selected_tier = sanitize_selected_tier(part, part.get("selected_tier"), default_tier="quality")
            source_key = f"job-{job_index}-part-{part_index}"
            existing = existing_map.get(source_key, {})
            tracker_items.append({
                "source_key": source_key,
                "source_type": "quote_part",
                "job_desc": job_desc,
                "part_desc": part_desc,
                "display_name": f"{part_desc} ({pretty_tier_name(selected_tier)})" if pretty_tier_name(selected_tier) else part_desc,
                "qty": max(safe_float(existing.get("qty", part.get("qty", 1)), part.get("qty", 1)), 0),
                "status": normalize_parts_tracker_status(existing.get("status")),
                "vendor": (existing.get("vendor") or "").strip(),
                "part_number": (existing.get("part_number") or "").strip(),
                "cost": safe_float(existing.get("cost", 0), 0),
                "notes": (existing.get("notes") or "").strip(),
                "selected_tier": selected_tier,
            })

    for item in existing_items:
        if not isinstance(item, dict):
            continue
        if (item.get("source_type") or "manual") != "manual":
            continue
        tracker_items.append({
            "source_key": (item.get("source_key") or f"manual-{secrets.token_hex(4)}").strip(),
            "source_type": "manual",
            "job_desc": (item.get("job_desc") or "").strip(),
            "part_desc": (item.get("part_desc") or "").strip(),
            "display_name": (item.get("display_name") or item.get("part_desc") or "").strip(),
            "qty": max(safe_float(item.get("qty", 1), 1), 0),
            "status": normalize_parts_tracker_status(item.get("status")),
            "vendor": (item.get("vendor") or "").strip(),
            "part_number": (item.get("part_number") or "").strip(),
            "cost": safe_float(item.get("cost", 0), 0),
            "notes": (item.get("notes") or "").strip(),
            "selected_tier": (item.get("selected_tier") or "").strip(),
        })

    return tracker_items


def build_parts_tracker_summary(parts_items):
    summary = {key: 0 for key in PARTS_TRACKER_STATUSES}
    for item in parts_items:
        if isinstance(item, dict):
            summary[normalize_parts_tracker_status(item.get("status"))] += 1
    return summary


def serialize_parts_tracker_from_form(form):
    fields = {
        "source_key": form.getlist("tracker_source_key[]"),
        "source_type": form.getlist("tracker_source_type[]"),
        "job_desc": form.getlist("tracker_job_desc[]"),
        "part_desc": form.getlist("tracker_part_desc[]"),
        "display_name": form.getlist("tracker_display_name[]"),
        "qty": form.getlist("tracker_qty[]"),
        "status": form.getlist("tracker_status[]"),
        "vendor": form.getlist("tracker_vendor[]"),
        "part_number": form.getlist("tracker_part_number[]"),
        "cost": form.getlist("tracker_cost[]"),
        "notes": form.getlist("tracker_notes[]"),
        "selected_tier": form.getlist("tracker_selected_tier[]"),
    }
    max_len = max((len(v) for v in fields.values()), default=0)
    items = []
    for i in range(max_len):
        source_type = (fields["source_type"][i].strip() if i < len(fields["source_type"]) else "quote_part") or "quote_part"
        source_key = (fields["source_key"][i].strip() if i < len(fields["source_key"]) else "")
        job_desc = fields["job_desc"][i].strip() if i < len(fields["job_desc"]) else ""
        part_desc = fields["part_desc"][i].strip() if i < len(fields["part_desc"]) else ""
        display_name = fields["display_name"][i].strip() if i < len(fields["display_name"]) else ""
        qty = max(safe_float(fields["qty"][i] if i < len(fields["qty"]) else 1, 1), 0)
        status = normalize_parts_tracker_status(fields["status"][i] if i < len(fields["status"]) else "need_to_order")
        vendor = fields["vendor"][i].strip() if i < len(fields["vendor"]) else ""
        part_number = fields["part_number"][i].strip() if i < len(fields["part_number"]) else ""
        cost = safe_float(fields["cost"][i] if i < len(fields["cost"]) else 0, 0)
        notes = fields["notes"][i].strip() if i < len(fields["notes"]) else ""
        selected_tier = fields["selected_tier"][i].strip() if i < len(fields["selected_tier"]) else ""

        if source_type == "manual" and not any([job_desc, part_desc, display_name, vendor, part_number, notes, qty, cost]):
            continue
        if not source_key:
            source_key = f"manual-{secrets.token_hex(4)}" if source_type == "manual" else f"quote-part-{i}"

        items.append({
            "source_key": source_key,
            "source_type": source_type,
            "job_desc": job_desc,
            "part_desc": part_desc,
            "display_name": display_name or part_desc,
            "qty": qty,
            "status": status,
            "vendor": vendor,
            "part_number": part_number,
            "cost": cost,
            "notes": notes,
            "selected_tier": selected_tier,
        })
    return json.dumps(items)


def get_job_parts_total(job, tier="quality"):
    total = 0.0
    for part in normalize_job_parts(job):
        qty = safe_float(part.get("qty", 1), 1)
        resolved_tier = sanitize_selected_tier(part, tier, default_tier="quality")
        price = safe_float(part.get(resolved_tier, 0))
        total += qty * price
    return round(total, 2)


def get_job_parts_total_from_selections(job, selected_parts=None, default_tier="quality"):
    parts = normalize_job_parts(job)
    if not parts:
        return 0.0
    selections_map = {}
    if isinstance(selected_parts, list):
        for item in selected_parts:
            if not isinstance(item, dict):
                continue
            try:
                part_index = int(item.get("part_index"))
            except (TypeError, ValueError):
                continue
            selections_map[part_index] = (item.get("tier") or default_tier or "quality").strip().lower()
    total = 0.0
    for idx, part in enumerate(parts):
        qty = safe_float(part.get("qty", 1), 1)
        tier = sanitize_selected_tier(part, selections_map.get(idx), default_tier=default_tier)
        price = safe_float(part.get(tier, 0))
        total += qty * price
    return round(total, 2)


def get_default_selected_parts(job, default_tier="quality"):
    return [{"part_index": idx, "tier": sanitize_selected_tier(part, part.get("selected_tier"), default_tier)} for idx, part in enumerate(normalize_job_parts(job))]


def parse_approved_map(approved_json):
    raw = json.loads(approved_json) if approved_json else []
    if isinstance(raw, dict):
        approved_jobs = raw.get("approved_jobs", [])
    elif isinstance(raw, list):
        approved_jobs = raw
    else:
        approved_jobs = []

    approved_map = {}

    for item in approved_jobs:
        if isinstance(item, dict):
            try:
                idx = int(item.get("job_index"))
            except (TypeError, ValueError):
                continue

            tier = (item.get("tier") or "quality").strip().lower()
            if tier not in ("oem", "quality", "economy"):
                tier = "quality"

            selected_parts = item.get("selected_parts")
            if not isinstance(selected_parts, list):
                selected_parts = []

            approved_map[idx] = {
                "tier": tier,
                "selected_parts": selected_parts,
            }
        else:
            try:
                idx = int(item)
            except (TypeError, ValueError):
                continue

            approved_map[idx] = {
                "tier": "quality",
                "selected_parts": [],
            }

    return approved_jobs, approved_map


def build_quote_totals(quote, payload, approved_map=None):
    jobs = payload.get("jobs", [])
    labor_rate = safe_float(quote["labor_rate"], 0)
    tax_rate = safe_float(quote["tax_rate"], 0)
    service_fee = safe_float(quote["service_fee"], 0)

    subtotal_labor = 0.0
    subtotal_parts = 0.0

    for idx, job in enumerate(jobs):
        labor_hours = safe_float(job.get("labor_hours", 0))

        if approved_map is not None:
            if idx not in approved_map:
                continue
            approved_item = approved_map[idx]
            selected_tier = approved_item.get("tier", "quality") if isinstance(approved_item, dict) else "quality"
            selected_parts = approved_item.get("selected_parts", []) if isinstance(approved_item, dict) else []
        else:
            selected_tier = "quality"
            selected_parts = []

        subtotal_labor += labor_hours * labor_rate
        subtotal_parts += get_job_parts_total_from_selections(job, selected_parts, default_tier=selected_tier)

    subtotal = subtotal_labor + subtotal_parts + service_fee
    tax = subtotal_parts * (tax_rate / 100.0)
    grand_total = subtotal + tax

    return {
        "subtotal_labor": round(subtotal_labor, 2),
        "subtotal_parts": round(subtotal_parts, 2),
        "service_fee": round(service_fee, 2),
        "subtotal": round(subtotal, 2),
        "tax": round(tax, 2),
        "grand_total": round(grand_total, 2),
        "labor_total": round(subtotal_labor, 2),
        "parts_total": round(subtotal_parts, 2),
        "tax_amount": round(tax, 2),
        "total": round(grand_total, 2),
    }



def row_get(row, key, default=None):
    try:
        if isinstance(row, sqlite3.Row):
            return row[key] if key in row.keys() else default
        if isinstance(row, dict):
            return row.get(key, default)
    except Exception:
        return default
    return default


def build_quote_profit_summary(quote_row):
    """Admin-only profit math. Never rendered on customer quote/invoice pages."""
    try:
        payload = json.loads(row_get(quote_row, "payload_json", "{}") or "{}")
    except Exception:
        payload = {}

    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    _, approved_map = parse_approved_map(row_get(quote_row, "approved_json"))
    has_approval = bool(approved_map)
    labor_rate = safe_float(row_get(quote_row, "labor_rate"), 0)
    service_fee = safe_float(row_get(quote_row, "service_fee"), 0)

    tracker_items = build_quote_parts_tracker(
        row_get(quote_row, "payload_json"),
        row_get(quote_row, "parts_tracking_json"),
    )
    tracker_map = {
        item.get("source_key"): item
        for item in tracker_items
        if isinstance(item, dict) and item.get("source_key")
    }

    parts_sale_total = 0.0
    parts_cost_total = 0.0
    parts_profit_total = 0.0
    labor_profit_total = 0.0
    manual_cost_total = 0.0
    missing_cost_count = 0
    profit_items = []

    for job_index, job in enumerate(jobs):
        if not isinstance(job, dict):
            continue

        if has_approval and job_index not in approved_map:
            continue

        approved_item = approved_map.get(job_index, {}) if has_approval else {}
        default_tier = approved_item.get("tier", "quality") if isinstance(approved_item, dict) else "quality"
        selected_parts = approved_item.get("selected_parts", []) if isinstance(approved_item, dict) else []
        selections_map = {}
        if isinstance(selected_parts, list):
            for selected in selected_parts:
                if not isinstance(selected, dict):
                    continue
                try:
                    selections_map[int(selected.get("part_index"))] = (selected.get("tier") or default_tier or "quality").strip().lower()
                except (TypeError, ValueError):
                    continue

        labor_profit_total += safe_float(job.get("labor_hours", 0), 0) * labor_rate

        for part_index, part in enumerate(normalize_job_parts(job)):
            source_key = f"job-{job_index}-part-{part_index}"
            tracker_item = tracker_map.get(source_key, {})
            qty = safe_float(part.get("qty", 1), 1)
            selected_tier = sanitize_selected_tier(
                part,
                selections_map.get(part_index) or part.get("selected_tier") or default_tier,
                default_tier="quality",
            )
            sale_total = qty * safe_float(part.get(selected_tier, 0), 0)
            cost_total = safe_float(tracker_item.get("cost", 0), 0)
            part_profit = sale_total - cost_total

            parts_sale_total += sale_total
            parts_cost_total += cost_total
            parts_profit_total += part_profit
            if sale_total > 0 and cost_total <= 0:
                missing_cost_count += 1

            profit_items.append({
                "source_key": source_key,
                "job_desc": (job.get("desc") or "Service").strip() or "Service",
                "part_desc": part.get("part_desc") or "Part",
                "selected_tier": selected_tier,
                "qty": round(qty, 2),
                "sale_total": round(sale_total, 2),
                "cost_total": round(cost_total, 2),
                "profit": round(part_profit, 2),
            })

    for item in tracker_items:
        if not isinstance(item, dict):
            continue
        if (item.get("source_type") or "quote_part") == "manual":
            cost_total = safe_float(item.get("cost", 0), 0)
            manual_cost_total += cost_total
            if cost_total:
                profit_items.append({
                    "source_key": item.get("source_key") or "manual",
                    "job_desc": item.get("job_desc") or "Manual/Internal",
                    "part_desc": item.get("display_name") or item.get("part_desc") or "Internal-only part",
                    "selected_tier": "manual",
                    "qty": safe_float(item.get("qty", 1), 1),
                    "sale_total": 0.0,
                    "cost_total": round(cost_total, 2),
                    "profit": round(-cost_total, 2),
                })

    parts_cost_total += manual_cost_total
    parts_profit_total -= manual_cost_total
    total_profit = parts_profit_total + labor_profit_total + service_fee
    profit_margin = (total_profit / (parts_sale_total + labor_profit_total + service_fee) * 100.0) if (parts_sale_total + labor_profit_total + service_fee) > 0 else 0.0
    parts_margin = (parts_profit_total / parts_sale_total * 100.0) if parts_sale_total > 0 else 0.0

    return {
        "parts_sale_total": round(parts_sale_total, 2),
        "parts_cost_total": round(parts_cost_total, 2),
        "parts_profit_total": round(parts_profit_total, 2),
        "labor_profit_total": round(labor_profit_total, 2),
        "service_fee_profit": round(service_fee, 2),
        "total_profit": round(total_profit, 2),
        "profit_margin": round(profit_margin, 1),
        "parts_margin": round(parts_margin, 1),
        "manual_cost_total": round(manual_cost_total, 2),
        "missing_cost_count": missing_cost_count,
        "items": profit_items,
    }


def empty_profit_summary():
    return {
        "parts_sale_total": 0.0,
        "parts_cost_total": 0.0,
        "parts_profit_total": 0.0,
        "labor_profit_total": 0.0,
        "service_fee_profit": 0.0,
        "total_profit": 0.0,
        "profit_margin": 0.0,
        "parts_margin": 0.0,
        "manual_cost_total": 0.0,
        "missing_cost_count": 0,
        "items": [],
    }




def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"png", "jpg", "jpeg", "webp", "gif", "heic", "heif"}


def quote_upload_dir(token):
    safe_token = slugify(token)
    path = os.path.join(UPLOADS_DIR, safe_token)
    os.makedirs(path, exist_ok=True)
    return path


def load_inspection(quote):
    raw = quote["inspection_json"] if isinstance(quote, sqlite3.Row) else quote.get("inspection_json")
    try:
        data = json.loads(raw or "{}")
    except json.JSONDecodeError:
        data = {}
    items = data.get("items", []) if isinstance(data, dict) else []
    cleaned = []
    for item in items:
        if not isinstance(item, dict):
            continue
        cleaned.append({
            "label": (item.get("label") or "").strip(),
            "status": (item.get("status") or "good").strip().lower() or "good",
            "notes": (item.get("notes") or "").strip(),
            "photo": (item.get("photo") or "").strip(),
            "estimate_added": bool(item.get("estimate_added", False)),
        })
    return {"items": cleaned}


def save_inspection_json(conn, quote_id, inspection_data):
    conn.execute("UPDATE quotes SET inspection_json = ? WHERE id = ?", (json.dumps(inspection_data), quote_id))
    conn.commit()


def build_inspection_from_request(form, files, token=None):
    labels = form.getlist("inspection_label[]")
    statuses = form.getlist("inspection_status[]")
    notes_list = form.getlist("inspection_notes[]")
    existing_photos = form.getlist("inspection_existing_photo[]")
    uploads = files.getlist("inspection_photo[]") if files else []
    items = []
    max_len = max(len(labels), len(statuses), len(notes_list), len(existing_photos), len(uploads)) if any([labels, statuses, notes_list, existing_photos, uploads]) else 0
    upload_dir = quote_upload_dir(token) if token else None

    for idx in range(max_len):
        label = labels[idx].strip() if idx < len(labels) else ""
        status = (statuses[idx].strip().lower() if idx < len(statuses) else "good") or "good"
        if status not in ("good", "monitor", "needs_attention"):
            status = "good"
        notes = notes_list[idx].strip() if idx < len(notes_list) else ""
        photo = existing_photos[idx].strip() if idx < len(existing_photos) else ""
        upload = uploads[idx] if idx < len(uploads) else None
        if upload and getattr(upload, 'filename', None) and allowed_file(upload.filename) and upload_dir:
            ext = upload.filename.rsplit('.', 1)[1].lower()
            fname = f"inspection_{idx}_{secrets.token_hex(6)}.{ext}"
            save_path = os.path.join(upload_dir, fname)
            upload.save(save_path)
            photo = f"/static/uploads/{slugify(token)}/{fname}"
        if not label and not notes and not photo:
            continue
        items.append({
            "label": label,
            "status": status,
            "notes": notes,
            "photo": photo,
            "estimate_added": False,
        })

    return {"items": items}


def get_saved_presets(conn):
    rows = conn.execute(
        "SELECT preset_key, preset_name, payload_json, is_system FROM saved_job_presets ORDER BY LOWER(preset_name) ASC, id ASC"
    ).fetchall()

    custom_presets = {}
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            continue

        jobs = normalize_preset_jobs(payload.get("jobs", []))
        if not jobs:
            continue

        custom_presets[row["preset_key"]] = {
            "name": (payload.get("name") or row["preset_name"] or "Saved Preset").strip(),
            "jobs": jobs,
            "is_custom": not bool(row["is_system"]),
        }

    return custom_presets


def get_all_job_presets(conn):
    presets = {key: {**value, "is_custom": False} for key, value in JOB_PRESETS.items()}
    presets.update(get_saved_presets(conn))
    return presets


def get_request_quote(conn, request_id):
    try:
        request_id_int = int(request_id)
    except (TypeError, ValueError):
        return None

    return conn.execute(
        "SELECT * FROM request_quotes WHERE id = ?",
        (request_id_int,),
    ).fetchone()


def upsert_saved_preset(conn, preset_name, jobs):
    preset_name = (preset_name or '').strip()
    normalized_jobs = normalize_preset_jobs(jobs)
    if not preset_name:
        raise ValueError('Preset name is required.')
    if not normalized_jobs:
        raise ValueError('Preset needs at least one valid service.')

    base_key = slugify(preset_name)
    preset_key = f"custom_{base_key}"
    if preset_key in JOB_PRESETS:
        preset_key = f"custom_{base_key}_{int(datetime.now().timestamp())}"

    payload = {
        "name": preset_name,
        "jobs": normalized_jobs,
    }
    now = datetime.now().isoformat()

    existing = conn.execute(
        "SELECT id, preset_key FROM saved_job_presets WHERE LOWER(preset_name) = LOWER(?)",
        (preset_name,),
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE saved_job_presets SET payload_json = ?, updated_at = ?, is_system = 0 WHERE id = ?",
            (json.dumps(payload), now, existing["id"]),
        )
        conn.commit()
        return existing["preset_key"]

    conn.execute(
        "INSERT INTO saved_job_presets (preset_key, preset_name, payload_json, created_at, updated_at, is_system) VALUES (?, ?, ?, ?, ?, 0)",
        (preset_key, preset_name, json.dumps(payload), now, now),
    )
    conn.commit()
    return preset_key

def get_customers_and_vehicles(conn):
    customers = conn.execute(
        "SELECT * FROM customers ORDER BY name COLLATE NOCASE ASC, created_at DESC"
    ).fetchall()
    vehicles = conn.execute(
        """
        SELECT
            vehicles.*,
            customers.name AS customer_name
        FROM vehicles
        LEFT JOIN customers ON customers.id = vehicles.customer_id
        ORDER BY customers.name COLLATE NOCASE ASC, vehicles.vehicle_text COLLATE NOCASE ASC
        """
    ).fetchall()
    return customers, vehicles




def build_estimate_builder_context(conn, quote_row=None, request_prefill=None):
    customers, vehicles = get_customers_and_vehicles(conn)
    all_job_presets = get_all_job_presets(conn)

    existing_quote = dict(quote_row) if quote_row else None
    request_prefill = dict(request_prefill) if request_prefill else None
    initial_jobs = []
    initial_inspection_items = []

    if existing_quote:
        payload = json.loads(existing_quote.get("payload_json") or "{}")
        initial_jobs = [
            {**job, "parts": normalize_job_parts(job)}
            for job in payload.get("jobs", [])
        ]
        initial_inspection_items = load_inspection(existing_quote).get("items", [])

    return {
        "customers": customers,
        "vehicles": vehicles,
        "default_labor_rate": safe_float(existing_quote["labor_rate"], DEFAULT_LABOR_RATE) if existing_quote else DEFAULT_LABOR_RATE,
        "default_tax_rate": safe_float(existing_quote["tax_rate"], DEFAULT_TAX_RATE) if existing_quote else DEFAULT_TAX_RATE,
        "default_service_fee": safe_float(existing_quote["service_fee"], DEFAULT_SERVICE_FEE) if existing_quote else DEFAULT_SERVICE_FEE,
        "job_presets_json": json.dumps(all_job_presets),
        "edit_mode": bool(existing_quote),
        "quote_token": existing_quote.get("quote_token", "") if existing_quote else "",
        "existing_quote": existing_quote,
        "request_prefill": request_prefill,
        "initial_request_id": request_prefill.get("id", "") if request_prefill else "",
        "initial_request_service": request_prefill.get("requested_service", "") if request_prefill else "",
        "existing_quote": existing_quote,
        "initial_jobs_json": json.dumps(initial_jobs),
        "initial_inspection_items_json": json.dumps(initial_inspection_items),
    }


@app.route("/", methods=["GET"])
def index():
    conn = get_db()
    request_prefill = get_request_quote(conn, request.args.get("request_id")) if request.args.get("request_id") else None
    context = build_estimate_builder_context(conn, request_prefill=request_prefill)
    conn.close()
    return render_template("index.html", **context)


@app.route("/edit_quote/<token>", methods=["GET"])
def edit_quote(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()

    if not quote:
        conn.close()
        abort(404)

    if quote["status"] in ("approved", "invoiced"):
        conn.close()
        flash("Approved or invoiced quotes are locked and cannot be edited.")
        return redirect(url_for("admin"))

    context = build_estimate_builder_context(conn, quote)
    conn.close()
    return render_template("index.html", **context)


@app.route("/save_preset", methods=["POST"])
def save_preset():
    data = request.get_json(silent=True) or {}
    preset_name = (data.get("name") or "").strip()
    jobs = data.get("jobs", [])

    conn = get_db()
    try:
        preset_key = upsert_saved_preset(conn, preset_name, jobs)
        all_presets = get_all_job_presets(conn)
    except ValueError as exc:
        conn.close()
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        conn.close()
        return jsonify({"ok": False, "error": "Preset could not be saved."}), 500

    conn.close()
    return jsonify({
        "ok": True,
        "message": f'Preset "{preset_name}" saved.',
        "preset_key": preset_key,
        "presets": all_presets,
    })


@app.route("/save_quote", methods=["POST"])
def save_quote():
    quote_token_input = request.form.get("quote_token", "").strip()
    existing_quote = None

    customer_name = request.form.get("customer_name", "").strip()
    customer_phone = request.form.get("customer_phone", "").strip()
    customer_email = request.form.get("customer_email", "").strip()
    vehicle = request.form.get("vehicle", "").strip()
    vin = request.form.get("vin", "").strip().upper()
    labor_rate = safe_float(request.form.get("labor_rate"), DEFAULT_LABOR_RATE)
    tax_rate = safe_float(request.form.get("tax_rate"), DEFAULT_TAX_RATE)
    service_fee = safe_float(request.form.get("service_fee"), DEFAULT_SERVICE_FEE)

    job_desc = request.form.getlist("job_desc[]")
    job_labor_hrs = request.form.getlist("job_labor_hrs[]")
    job_notes = request.form.getlist("job_notes[]")

    jobs = []
    max_len = max(len(job_desc), len(job_labor_hrs), len(job_notes)) if any([job_desc, job_labor_hrs, job_notes]) else 0

    for i in range(max_len):
        desc = job_desc[i].strip() if i < len(job_desc) else ""
        labor_hours = safe_float(job_labor_hrs[i] if i < len(job_labor_hrs) else 0, 0)
        notes = job_notes[i].strip() if i < len(job_notes) else ""

        part_descs = request.form.getlist(f"part_desc_{i}[]")
        part_qtys = request.form.getlist(f"part_qty_{i}[]")
        part_oems = request.form.getlist(f"part_oem_{i}[]")
        part_qualities = request.form.getlist(f"part_quality_{i}[]")
        part_economies = request.form.getlist(f"part_economy_{i}[]")
        part_list_oems = request.form.getlist(f"part_list_oem_{i}[]")
        part_list_qualities = request.form.getlist(f"part_list_quality_{i}[]")
        part_list_economies = request.form.getlist(f"part_list_economy_{i}[]")
        part_number_oems = request.form.getlist(f"part_number_oem_{i}[]")
        part_number_qualities = request.form.getlist(f"part_number_quality_{i}[]")
        part_number_economies = request.form.getlist(f"part_number_economy_{i}[]")
        part_source_oems = request.form.getlist(f"part_source_oem_{i}[]")
        part_source_qualities = request.form.getlist(f"part_source_quality_{i}[]")
        part_source_economies = request.form.getlist(f"part_source_economy_{i}[]")
        part_buffer_oems = request.form.getlist(f"part_buffer_oem_{i}[]")
        part_buffer_qualities = request.form.getlist(f"part_buffer_quality_{i}[]")
        part_buffer_economies = request.form.getlist(f"part_buffer_economy_{i}[]")
        part_markup_oems = request.form.getlist(f"part_markup_oem_{i}[]")
        part_markup_qualities = request.form.getlist(f"part_markup_quality_{i}[]")
        part_markup_economies = request.form.getlist(f"part_markup_economy_{i}[]")
        enabled_oems = request.form.getlist(f"part_enabled_oem_{i}[]")
        enabled_qualities = request.form.getlist(f"part_enabled_quality_{i}[]")
        enabled_economies = request.form.getlist(f"part_enabled_economy_{i}[]")
        selected_tiers = request.form.getlist(f"part_selected_tier_{i}[]")

        parts = []
        part_max_len = max(
            len(part_descs), len(part_qtys), len(part_oems), len(part_qualities), len(part_economies),
            len(part_list_oems), len(part_list_qualities), len(part_list_economies),
            len(part_number_oems), len(part_number_qualities), len(part_number_economies),
            len(part_source_oems), len(part_source_qualities), len(part_source_economies),
            len(part_buffer_oems), len(part_buffer_qualities), len(part_buffer_economies),
            len(part_markup_oems), len(part_markup_qualities), len(part_markup_economies),
            len(enabled_oems), len(enabled_qualities), len(enabled_economies), len(selected_tiers)
        ) if any([part_descs, part_qtys, part_oems, part_qualities, part_economies, part_list_oems, part_list_qualities, part_list_economies, part_number_oems, part_number_qualities, part_number_economies, part_source_oems, part_source_qualities, part_source_economies, part_buffer_oems, part_buffer_qualities, part_buffer_economies, part_markup_oems, part_markup_qualities, part_markup_economies, enabled_oems, enabled_qualities, enabled_economies, selected_tiers]) else 0

        for p in range(part_max_len):
            part_desc = part_descs[p].strip() if p < len(part_descs) else ""
            qty = safe_float(part_qtys[p] if p < len(part_qtys) else 1, 1)
            oem = safe_float(part_oems[p] if p < len(part_oems) else 0, 0)
            quality = safe_float(part_qualities[p] if p < len(part_qualities) else 0, 0)
            economy = safe_float(part_economies[p] if p < len(part_economies) else 0, 0)

            list_oem = safe_float(part_list_oems[p] if p < len(part_list_oems) else oem, oem)
            list_quality = safe_float(part_list_qualities[p] if p < len(part_list_qualities) else quality, quality)
            list_economy = safe_float(part_list_economies[p] if p < len(part_list_economies) else economy, economy)
            oem_part_number = part_number_oems[p].strip() if p < len(part_number_oems) else ""
            quality_part_number = part_number_qualities[p].strip() if p < len(part_number_qualities) else ""
            economy_part_number = part_number_economies[p].strip() if p < len(part_number_economies) else ""
            source_oem = (part_source_oems[p].strip() if p < len(part_source_oems) else "online_oem") or "online_oem"
            source_quality = (part_source_qualities[p].strip() if p < len(part_source_qualities) else "online_oem") or "online_oem"
            source_economy = (part_source_economies[p].strip() if p < len(part_source_economies) else "online_oem") or "online_oem"
            buffer_oem = safe_float(part_buffer_oems[p] if p < len(part_buffer_oems) else (0 if source_oem == "dealer_local" else 25), 25)
            buffer_quality = safe_float(part_buffer_qualities[p] if p < len(part_buffer_qualities) else (0 if source_quality == "dealer_local" else 25), 25)
            buffer_economy = safe_float(part_buffer_economies[p] if p < len(part_buffer_economies) else (0 if source_economy == "dealer_local" else 25), 25)
            markup_oem = safe_float(part_markup_oems[p] if p < len(part_markup_oems) else 0, 0)
            markup_quality = safe_float(part_markup_qualities[p] if p < len(part_markup_qualities) else 0, 0)
            markup_economy = safe_float(part_markup_economies[p] if p < len(part_markup_economies) else 0, 0)
            enabled_oem = (enabled_oems[p].strip() == "1") if p < len(enabled_oems) else (oem > 0)
            enabled_quality = (enabled_qualities[p].strip() == "1") if p < len(enabled_qualities) else (quality > 0 or (not enabled_oem and economy <= 0))
            enabled_economy = (enabled_economies[p].strip() == "1") if p < len(enabled_economies) else (economy > 0)
            selected_tier = selected_tiers[p].strip().lower() if p < len(selected_tiers) else "quality"
            selected_tier = sanitize_selected_tier({
                "enabled_oem": enabled_oem,
                "enabled_quality": enabled_quality,
                "enabled_economy": enabled_economy,
            }, selected_tier, default_tier="quality")

            if not part_desc and not oem_part_number and not quality_part_number and not economy_part_number and qty == 1 and oem == 0 and quality == 0 and economy == 0 and list_oem == 0 and list_quality == 0 and list_economy == 0:
                continue

            parts.append({
                "part_desc": part_desc,
                "qty": qty,
                "oem": oem,
                "quality": quality,
                "economy": economy,
                "list_oem": list_oem,
                "list_quality": list_quality,
                "list_economy": list_economy,
                "oem_part_number": oem_part_number,
                "quality_part_number": quality_part_number,
                "economy_part_number": economy_part_number,
                "source_oem": source_oem,
                "source_quality": source_quality,
                "source_economy": source_economy,
                "buffer_oem": buffer_oem,
                "buffer_quality": buffer_quality,
                "buffer_economy": buffer_economy,
                "flat_markup_oem": markup_oem,
                "flat_markup_quality": markup_quality,
                "flat_markup_economy": markup_economy,
                "markup_oem": markup_oem,
                "markup_quality": markup_quality,
                "markup_economy": markup_economy,
                "enabled_oem": enabled_oem,
                "enabled_quality": enabled_quality,
                "enabled_economy": enabled_economy,
                "selected_tier": selected_tier,
            })

        if not desc and labor_hours == 0 and not notes and not parts:
            continue

        jobs.append({
            "desc": desc,
            "labor_hours": labor_hours,
            "notes": notes,
            "parts": parts,
        })

    payload = {"jobs": jobs}
    request_quote_id = request.form.get("request_quote_id", "").strip()

    conn = get_db()
    cur = conn.cursor()

    if quote_token_input:
        existing_quote = cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (quote_token_input,)).fetchone()
        if existing_quote and existing_quote["status"] in ("approved", "invoiced"):
            conn.close()
            flash("Approved or invoiced quotes are locked and cannot be edited.")
            return redirect(url_for("admin"))

    token = existing_quote["quote_token"] if existing_quote else generate_token()
    public_slug = (existing_quote["public_slug"] if existing_quote and "public_slug" in existing_quote.keys() else "") or generate_quote_public_slug(conn, customer_name, vehicle)
    inspection_data = build_inspection_from_request(request.form, request.files, token=token)

    customer_id = find_or_create_customer(conn, customer_name, customer_phone, customer_email)
    vehicle_id = find_or_create_vehicle(conn, customer_id, vehicle, vin) if customer_id else None

    if existing_quote:
        cur.execute(
            """
            UPDATE quotes
            SET customer_id = ?, vehicle_id = ?, customer_name = ?, customer_phone = ?,
                customer_email = ?, vehicle = ?, vin = ?, labor_rate = ?, tax_rate = ?,
                service_fee = ?, payload_json = ?, inspection_json = ?, public_slug = ?, status = 'quote',
                approved_json = NULL, signature_data = NULL, signed_name = NULL, signed_at = NULL
            WHERE quote_token = ?
            """,
            (
                customer_id,
                vehicle_id,
                customer_name,
                customer_phone,
                customer_email,
                vehicle,
                vin,
                labor_rate,
                tax_rate,
                service_fee,
                json.dumps(payload),
                json.dumps(inspection_data),
                public_slug,
                token,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO quotes (
                quote_token, public_slug, created_at, customer_id, vehicle_id, customer_name, customer_phone,
                customer_email, vehicle, vin, labor_rate, tax_rate, service_fee, payload_json, inspection_json, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                token,
                public_slug,
                datetime.now().isoformat(),
                customer_id,
                vehicle_id,
                customer_name,
                customer_phone,
                customer_email,
                vehicle,
                vin,
                labor_rate,
                tax_rate,
                service_fee,
                json.dumps(payload),
                json.dumps(inspection_data),
                "quote",
            ),
        )

    if request_quote_id:
        try:
            request_quote_id_int = int(request_quote_id)
            cur.execute(
                "UPDATE request_quotes SET status = 'quoted' WHERE id = ?",
                (request_quote_id_int,),
            )
        except (TypeError, ValueError):
            pass

    conn.commit()
    conn.close()
    return redirect(url_for("view_quote", token=public_slug or token))


@app.route("/quote/<token>")
def view_quote(token):
    conn = get_db()
    row = conn.execute("SELECT * FROM quotes WHERE quote_token = ? OR public_slug = ?", (token, token)).fetchone()
    conn.close()

    if row is None:
        return "Quote not found", 404

    quote = dict(row)
    payload = json.loads(quote["payload_json"] or "{}")
    jobs = payload.get("jobs", [])
    jobs = [{**job, "parts": normalize_job_parts(job)} for job in jobs]

    _approved_jobs, approved_map = parse_approved_map(quote.get("approved_json"))
    inspection = load_inspection(quote)
    return render_template("quote.html", quote=quote, jobs=jobs, approved_map=approved_map, inspection=inspection)




@app.route("/quote/<token>/pdf", methods=["GET"])
@app.route("/quote_pdf/<token>", methods=["GET"])
@app.route("/download_quote_pdf/<token>", methods=["GET"])
def download_quote_pdf(token):
    conn = get_db()
    row = get_quote_for_any_token(conn, token)
    conn.close()

    if not row:
        abort(404)

    pdf_bytes = render_quote_pdf_bytes(row)
    response = make_response(pdf_bytes)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'attachment; filename="{make_safe_attachment_name(row)}"'
    return response


@app.route("/admin/send_quote_email/<token>", methods=["POST"])
@app.route("/send_quote_email/<token>", methods=["POST"])
@admin_required
def send_quote_email(token):
    conn = get_db()
    quote = get_quote_for_any_token(conn, token)
    conn.close()
    if not quote:
        abort(404)

    try:
        pdf_bytes = render_quote_pdf_bytes(quote)
        send_quote_email_message(quote, pdf_bytes)
    except Exception as exc:
        flash(f"Quote email failed: {exc}")
        return redirect(url_for("admin"))

    flash(f"Quote emailed to {(quote['customer_email'] or '').strip()}.")
    return redirect(url_for("admin"))


@app.route("/approve_quote/<token>", methods=["POST"])
def approve_quote(token):
    approved_jobs = request.form.getlist("approve_job[]")
    signature_data = request.form.get("signature_data", "")
    signed_name = request.form.get("signed_name", "").strip()
    location_permission_confirm = request.form.get("location_permission_confirm", "").strip().lower()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,))
    quote = cur.fetchone()
    if not quote:
        conn.close()
        abort(404)

    payload = json.loads(quote["payload_json"] or "{}")
    jobs = payload.get("jobs", [])
    approved_payload = []

    for approved_idx in approved_jobs:
        try:
            idx = int(approved_idx)
        except (TypeError, ValueError):
            continue

        if idx < 0 or idx >= len(jobs):
            continue

        job = jobs[idx]
        parts = normalize_job_parts(job)
        selected_parts = []

        for part_index, part in enumerate(parts):
            requested_tier = request.form.get(f"part_tier_{idx}_{part_index}", "quality").strip().lower()
            selected_tier = sanitize_selected_tier(part, requested_tier, default_tier="quality")
            selected_parts.append({
                "part_index": part_index,
                "tier": selected_tier,
            })

        job_default_tier = selected_parts[0]["tier"] if selected_parts else "quality"

        approved_payload.append({
            "job_index": idx,
            "tier": job_default_tier,
            "selected_parts": selected_parts,
        })

    approval_meta = {
        "location_permission_confirm": location_permission_confirm == "yes",
    }

    cur.execute(
        """
        UPDATE quotes
        SET approved_json = ?, signature_data = ?, signed_name = ?, signed_at = ?, status = 'approved'
        WHERE quote_token = ?
        """,
        (
            json.dumps({
                "approved_jobs": approved_payload,
                "approval_meta": approval_meta,
            }),
            signature_data,
            signed_name,
            datetime.now().isoformat(),
            token,
        ),
    )

    conn.commit()
    conn.close()
    return redirect(url_for("view_quote", token=token))


@app.route("/convert_invoice/<token>", methods=["GET"])
@admin_required
def convert_invoice(token):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,))
    quote = cur.fetchone()
    if not quote:
        conn.close()
        abort(404)

    cur.execute("SELECT * FROM invoices WHERE quote_id = ?", (quote["id"],))
    existing_invoice = cur.fetchone()
    if existing_invoice:
        conn.close()
        return redirect(url_for("view_invoice", invoice_number=existing_invoice["invoice_number"]))

    payload = json.loads(quote["payload_json"] or "{}")
    _, approved_map = parse_approved_map(quote["approved_json"])

    labor_total = 0.0
    parts_total = 0.0
    tax_rate = safe_float(quote["tax_rate"], 0)
    service_fee = safe_float(quote["service_fee"], 0)

    for idx, approved_item in approved_map.items():
        if idx < 0 or idx >= len(payload.get("jobs", [])):
            continue

        job = payload["jobs"][idx]
        selected_tier = approved_item.get("tier", "quality") if isinstance(approved_item, dict) else "quality"
        selected_parts = approved_item.get("selected_parts", []) if isinstance(approved_item, dict) else []

        labor_total += safe_float(job.get("labor_hours", 0)) * safe_float(quote["labor_rate"], 0)
        parts_total += get_job_parts_total_from_selections(job, selected_parts, default_tier=selected_tier)

    tax_total = parts_total * (tax_rate / 100.0)
    grand_total = labor_total + parts_total + service_fee + tax_total
    invoice_number = generate_invoice_number()

    cur.execute(
        "INSERT INTO invoices (invoice_number, quote_id, created_at, total, payment_status) VALUES (?, ?, ?, ?, ?)",
        (invoice_number, quote["id"], datetime.now().isoformat(), round(grand_total, 2), "unpaid"),
    )
    cur.execute("UPDATE quotes SET status = 'invoiced' WHERE id = ?", (quote["id"],))

    conn.commit()
    conn.close()
    return redirect(url_for("view_invoice", invoice_number=invoice_number))


@app.route("/inspection/<token>", methods=["GET", "POST"])
def inspection(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()
    if not quote:
        abort(404)

    flash("Inspection editing is locked after the quote is saved. Inspection items can only be edited on the estimator page before saving the quote.")
    return redirect(url_for("view_quote", token=token))


@app.route("/inspection_add_to_estimate/<token>/<int:item_index>", methods=["POST"])
def inspection_add_to_estimate(token, item_index):
    flash("Inspection items can only be added or edited from the estimator before the quote is saved.")
    return redirect(url_for("view_quote", token=token))


@app.route("/invoice/<invoice_number>", methods=["GET"])
def view_invoice(invoice_number):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            invoices.id AS invoice_id,
            invoices.invoice_number,
            invoices.created_at AS invoice_created_at,
            invoices.total AS invoice_total,
            invoices.payment_status,
            invoices.payment_method,
            invoices.paid_at,
            quotes.*
        FROM invoices
        JOIN quotes ON quotes.id = invoices.quote_id
        WHERE invoices.invoice_number = ?
        """,
        (invoice_number,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        abort(404)

    payload = json.loads(row["payload_json"] or "{}")
    approved_jobs, approved_map = parse_approved_map(row["approved_json"])
    totals = build_quote_totals(row, payload, approved_map=approved_map)

    return render_template(
        "invoice.html",
        row=row,
        payload=payload,
        totals=totals,
        approved_jobs=approved_jobs,
        approved_map=approved_map,
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if admin_session_active():
        return redirect(url_for("admin"))

    next_url = request.args.get("next") or request.form.get("next") or url_for("admin")
    if request.method == "POST":
        password = (request.form.get("password") or "").strip()
        if password == ADMIN_PASSWORD:
            login_admin_session()
            flash("Admin login successful.")
            if next_url and next_url.startswith("/"):
                return redirect(next_url)
            return redirect(url_for("admin"))
        flash("Incorrect admin password.")

    return render_template("login.html", next_url=next_url, session_minutes=int(app.permanent_session_lifetime.total_seconds() // 60))


@app.route("/logout", methods=["GET"])
def logout():
    clear_admin_session()
    flash("Logged out.")
    return redirect(url_for("login"))



def normalize_admin_quote_status(value):
    value = (value or 'active').strip().lower().replace(' ', '_')
    allowed = {'active', 'waiting', 'archived'}
    return value if value in allowed else 'active'


def parse_iso_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00'))
    except Exception:
        return None


def auto_archive_waiting_quotes(conn):
    now = datetime.now()
    waiting_quotes = conn.execute(
        "SELECT id, waiting_since, signed_at, status, invoice_number FROM (SELECT quotes.id, quotes.waiting_since, quotes.signed_at, quotes.status, invoices.invoice_number FROM quotes LEFT JOIN invoices ON invoices.quote_id = quotes.id) WHERE status != 'invoiced'"
    ).fetchall()
    for row in waiting_quotes:
        quote = conn.execute("SELECT id, admin_status, waiting_since, signed_at, status FROM quotes WHERE id = ?", (row['id'],)).fetchone()
        if not quote:
            continue
        admin_status = normalize_admin_quote_status(quote['admin_status'] if 'admin_status' in quote.keys() else 'active')
        if admin_status != 'waiting':
            continue
        if (quote['status'] or '').strip().lower() in {'approved', 'invoiced'}:
            continue
        if quote['signed_at']:
            continue
        waiting_since_dt = parse_iso_datetime(quote['waiting_since']) or parse_iso_datetime(conn.execute("SELECT created_at FROM quotes WHERE id = ?", (quote['id'],)).fetchone()['created_at'])
        if waiting_since_dt and now - waiting_since_dt >= timedelta(days=7):
            conn.execute(
                "UPDATE quotes SET admin_status = 'archived', archived_at = ?, waiting_since = COALESCE(waiting_since, created_at) WHERE id = ?",
                (now.isoformat(), quote['id'])
            )
    conn.commit()


def compute_quote_age_days(created_at):
    dt = parse_iso_datetime(created_at)
    if not dt:
        return None
    return max((datetime.now() - dt).days, 0)


def compute_quote_waiting_days(waiting_since):
    dt = parse_iso_datetime(waiting_since)
    if not dt:
        return None
    return max((datetime.now() - dt).days, 0)


@app.route("/admin", methods=["GET"])
@admin_required
def admin():
    selected_filter = normalize_admin_quote_status(request.args.get("view") or "active")

    conn = get_db()
    auto_archive_waiting_quotes(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            quotes.*,
            invoices.invoice_number,
            invoices.payment_status,
            invoices.payment_method,
            invoices.paid_at
        FROM quotes
        LEFT JOIN invoices ON invoices.quote_id = quotes.id
        ORDER BY quotes.created_at DESC
        """
    )
    raw_rows = cur.fetchall()
    request_rows_raw = cur.execute(
        "SELECT * FROM request_quotes ORDER BY created_at DESC, id DESC"
    ).fetchall()
    conn.close()

    rows = []
    status_counts = {"active": 0, "waiting": 0, "archived": 0, "all": 0}
    for row in raw_rows:
        row_dict = dict(row)
        admin_status = normalize_admin_quote_status(row_dict.get("admin_status"))
        quote_status = (row_dict.get("status") or "quote").strip().lower()
        if quote_status in {"approved", "invoiced"} and admin_status == "waiting":
            admin_status = "active"
        row_dict["admin_status"] = admin_status
        status_counts[admin_status] += 1
        status_counts["all"] += 1
        row_dict["quote_public_key"] = get_quote_public_key(row_dict)
        row_dict["quote_url_external"] = build_quote_url_external(row_dict) if row_dict.get("quote_public_key") else ""
        row_dict["customer_phone_display"] = display_phone_number(row["customer_phone"])
        row_dict["customer_phone_e164"] = normalize_phone_number(row["customer_phone"])
        row_dict["parts_tracker_items"] = build_quote_parts_tracker(row_dict.get("payload_json"), row_dict.get("parts_tracking_json"))
        row_dict["parts_tracker_summary"] = build_parts_tracker_summary(row_dict["parts_tracker_items"])
        row_dict["profit_summary"] = build_quote_profit_summary(row_dict)
        row_dict["age_days"] = compute_quote_age_days(row_dict.get("created_at"))
        row_dict["waiting_days"] = compute_quote_waiting_days(row_dict.get("waiting_since"))
        row_dict["is_auto_archive_candidate"] = admin_status == "waiting" and (row_dict["waiting_days"] is not None and row_dict["waiting_days"] >= 5) and quote_status not in {"approved", "invoiced"}
        row_dict["is_overdue_waiting"] = admin_status == "waiting" and (row_dict["waiting_days"] is not None and row_dict["waiting_days"] >= 7) and quote_status not in {"approved", "invoiced"}
        if selected_filter in {"all", admin_status}:
            rows.append(row_dict)

    profit_summary = empty_profit_summary()
    for row_dict in rows:
        row_profit = row_dict.get("profit_summary") or empty_profit_summary()
        profit_summary["parts_sale_total"] += safe_float(row_profit.get("parts_sale_total"), 0)
        profit_summary["parts_cost_total"] += safe_float(row_profit.get("parts_cost_total"), 0)
        profit_summary["parts_profit_total"] += safe_float(row_profit.get("parts_profit_total"), 0)
        profit_summary["labor_profit_total"] += safe_float(row_profit.get("labor_profit_total"), 0)
        profit_summary["service_fee_profit"] += safe_float(row_profit.get("service_fee_profit"), 0)
        profit_summary["total_profit"] += safe_float(row_profit.get("total_profit"), 0)
        profit_summary["manual_cost_total"] += safe_float(row_profit.get("manual_cost_total"), 0)
        profit_summary["missing_cost_count"] += safe_int(row_profit.get("missing_cost_count"), 0)

    gross_sales_for_margin = profit_summary["parts_sale_total"] + profit_summary["labor_profit_total"] + profit_summary["service_fee_profit"]
    profit_summary["profit_margin"] = round((profit_summary["total_profit"] / gross_sales_for_margin * 100.0), 1) if gross_sales_for_margin > 0 else 0.0
    profit_summary["parts_margin"] = round((profit_summary["parts_profit_total"] / profit_summary["parts_sale_total"] * 100.0), 1) if profit_summary["parts_sale_total"] > 0 else 0.0
    for key in ("parts_sale_total", "parts_cost_total", "parts_profit_total", "labor_profit_total", "service_fee_profit", "total_profit", "manual_cost_total"):
        profit_summary[key] = round(profit_summary[key], 2)

    request_rows = []
    for row in request_rows_raw:
        request_dict = dict(row)
        request_dict["customer_phone_display"] = display_phone_number(row["customer_phone"])
        request_dict["customer_phone_e164"] = normalize_phone_number(row["customer_phone"])
        request_rows.append(request_dict)

    return render_template(
        "admin_quotes.html",
        rows=rows,
        request_rows=request_rows,
        email_enabled=email_settings_ready(),
        selected_quote_view=selected_filter,
        quote_status_counts=status_counts,
        profit_summary=profit_summary,
    )

@app.route("/request-quote", methods=["GET", "POST"])
@app.route("/request_quote", methods=["GET", "POST"])
def request_quote():
    if request.method == "POST":
        customer_name = request.form.get("customer_name", "").strip()
        customer_phone = request.form.get("customer_phone", "").strip()
        customer_email = request.form.get("customer_email", "").strip()
        vehicle = request.form.get("vehicle", "").strip()
        vin = request.form.get("vin", "").strip().upper()
        requested_service = request.form.get("requested_service", "").strip()
        concern_details = request.form.get("concern_details", "").strip()
        preferred_schedule = request.form.get("preferred_schedule", "").strip()

        conn = get_db()
        conn.execute(
            """
            INSERT INTO request_quotes (
                created_at, customer_name, customer_phone, customer_email, vehicle, vin,
                requested_service, concern_details, preferred_schedule, status, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                customer_name,
                customer_phone,
                customer_email,
                vehicle,
                vin,
                requested_service,
                concern_details,
                preferred_schedule,
                "new",
                "website",
            ),
        )
        conn.commit()
        conn.close()
        return render_template("request_quote.html", submitted=True)

    return render_template("request_quote.html", submitted=False)


@app.route("/admin/request/<int:request_id>/use-in-estimator", methods=["GET"])
@admin_required
def use_request_in_estimator(request_id):
    conn = get_db()
    row = conn.execute("SELECT id FROM request_quotes WHERE id = ?", (request_id,)).fetchone()
    conn.close()
    if not row:
        abort(404)
    return redirect(url_for("index", request_id=request_id))


@app.route("/admin/request/<int:request_id>/status", methods=["POST"])
@admin_required
def update_request_quote_status(request_id):
    new_status = (request.form.get("status") or "new").strip().lower()
    allowed_statuses = {"new", "contacted", "quoted", "closed"}
    if new_status not in allowed_statuses:
        new_status = "new"

    conn = get_db()
    row = conn.execute("SELECT id FROM request_quotes WHERE id = ?", (request_id,)).fetchone()
    if not row:
        conn.close()
        abort(404)

    conn.execute("UPDATE request_quotes SET status = ? WHERE id = ?", (new_status, request_id))
    conn.commit()
    conn.close()
    flash(f"Request #{request_id} updated to {new_status}.")
    return redirect(url_for("admin"))


@app.route("/admin/request/<int:request_id>/delete", methods=["POST"])
@admin_required
def delete_request_quote(request_id):
    conn = get_db()
    row = conn.execute("SELECT id FROM request_quotes WHERE id = ?", (request_id,)).fetchone()
    if not row:
        conn.close()
        abort(404)

    conn.execute("DELETE FROM request_quotes WHERE id = ?", (request_id,))
    conn.commit()
    conn.close()
    flash(f"Request #{request_id} deleted.")
    return redirect(url_for("admin"))


@app.route("/mark_paid/<invoice_number>", methods=["POST"])
@admin_required
def mark_paid(invoice_number):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM invoices WHERE invoice_number = ?", (invoice_number,))
    invoice = cur.fetchone()
    if not invoice:
        conn.close()
        abort(404)

    cur.execute(
        "UPDATE invoices SET payment_status = 'paid', paid_at = ? WHERE invoice_number = ?",
        (datetime.now().isoformat(), invoice_number),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("view_invoice", invoice_number=invoice_number))


@app.route("/admin/copy_quote_text/<token>", methods=["GET"])
@admin_required
def copy_quote_text(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()
    if not quote:
        abort(404)

    return jsonify({
        "ok": True,
        "message": build_copy_text_message(quote),
        "text": build_copy_text_message(quote),
        "quote_url": build_quote_url_external(quote),
        "phone": normalize_phone_number(quote["customer_phone"]),
        "phone_display": display_phone_number(quote["customer_phone"]),
        "customer_name": quote["customer_name"] or "",
        "vehicle": quote["vehicle"] or "",
    })


@app.route("/send_quote_sms/<token>", methods=["POST"])
@admin_required
def send_quote_sms(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()
    if not quote:
        abort(404)
    flash("SMS sending is paused in this build. Use Copy Text to send the quote link manually.")
    return redirect(url_for("admin"))


@app.route("/admin/quote/<int:quote_id>/admin_status", methods=["POST"])
@admin_required
def update_quote_admin_status(quote_id):
    new_status = normalize_admin_quote_status(request.form.get("admin_status") or "active")
    now = datetime.now().isoformat()
    conn = get_db()
    cur = conn.cursor()
    quote = cur.execute("SELECT * FROM quotes WHERE id = ?", (quote_id,)).fetchone()
    if not quote:
        conn.close()
        abort(404)

    updates = ["admin_status = ?"]
    params = [new_status]

    if new_status == "waiting":
        updates.append("waiting_since = ?")
        params.append(now)
        updates.append("archived_at = NULL")
    elif new_status == "archived":
        updates.append("archived_at = ?")
        params.append(now)
    else:
        updates.append("archived_at = NULL")

    if new_status == "active":
        updates.append("waiting_since = NULL")
    elif new_status == "archived":
        updates.append("waiting_since = COALESCE(waiting_since, created_at)")

    params.append(quote_id)
    cur.execute(f"UPDATE quotes SET {', '.join(updates)} WHERE id = ?", params)
    conn.commit()
    conn.close()
    flash(f"Quote moved to {new_status.replace('_', ' ')}.")
    return redirect(url_for("admin", view=request.args.get("view") or request.form.get("return_view") or "active"))


@app.route("/admin/update_parts_tracker/<int:quote_id>", methods=["POST"])
@admin_required
def update_parts_tracker(quote_id):
    conn = get_db()
    cur = conn.cursor()
    quote = cur.execute("SELECT * FROM quotes WHERE id = ?", (quote_id,)).fetchone()
    if not quote:
        conn.close()
        abort(404)

    cur.execute(
        "UPDATE quotes SET parts_tracking_json = ? WHERE id = ?",
        (serialize_parts_tracker_from_form(request.form), quote_id),
    )
    conn.commit()
    conn.close()
    flash("Parts tracker updated.")
    return redirect(url_for("admin"))


@app.route("/admin/delete_invoice/<invoice_number>", methods=["POST"])
@admin_required
def delete_invoice(invoice_number):
    conn = get_db()
    cur = conn.cursor()
    invoice = cur.execute("SELECT * FROM invoices WHERE invoice_number = ?", (invoice_number,)).fetchone()
    if not invoice:
        conn.close()
        abort(404)

    cur.execute("DELETE FROM invoices WHERE invoice_number = ?", (invoice_number,))
    if invoice["quote_id"]:
        cur.execute("UPDATE quotes SET status = 'approved' WHERE id = ? AND status = 'invoiced'", (invoice["quote_id"],))
    conn.commit()
    conn.close()
    flash(f"Invoice {invoice_number} deleted.")
    return redirect(url_for("admin"))


@app.route("/admin/clear_approval/<token>", methods=["POST"])
@admin_required
def clear_approval(token):
    conn = get_db()
    cur = conn.cursor()
    quote = cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    if not quote:
        conn.close()
        abort(404)

    existing_invoice = cur.execute("SELECT id FROM invoices WHERE quote_id = ?", (quote["id"],)).fetchone()
    if existing_invoice:
        conn.close()
        flash("Delete the invoice first before clearing approval on this quote.")
        return redirect(url_for("admin"))

    cur.execute(
        """
        UPDATE quotes
        SET approved_json = NULL,
            signature_data = NULL,
            signed_name = NULL,
            signed_at = NULL,
            status = 'quote'
        WHERE quote_token = ?
        """,
        (token,),
    )
    conn.commit()
    conn.close()
    flash("Approval cleared.")
    return redirect(url_for("admin"))


@app.route("/delete_quote/<int:quote_id>", methods=["POST"])
@app.route("/admin/delete_quote/<int:quote_id>", methods=["POST"])
@admin_required
def delete_quote(quote_id):
    conn = get_db()
    cur = conn.cursor()
    quote = cur.execute("SELECT * FROM quotes WHERE id = ?", (quote_id,)).fetchone()
    if not quote:
        conn.close()
        abort(404)

    cur.execute("DELETE FROM invoices WHERE quote_id = ?", (quote_id,))
    cur.execute("DELETE FROM quotes WHERE id = ?", (quote_id,))
    conn.commit()
    conn.close()
    flash("Quote deleted.")
    return redirect(url_for("admin"))


if __name__ == "__main__":
    app.run(debug=True)

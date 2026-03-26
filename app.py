import os
import json
import sqlite3
import secrets
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, abort, flash
from twilio.rest import Client

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(DEFAULT_DATA_DIR, "quotes.db"))

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "").strip()
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").strip().rstrip("/")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
app.secret_key = secrets.token_hex(16)

DEFAULT_LABOR_RATE = 150.00
DEFAULT_TAX_RATE = 7.25
DEFAULT_SERVICE_FEE = 0.00


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def safe_float(value, default=0.0):
    try:
        return float(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        return default


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

    conn.commit()

    add_column_if_missing(conn, "quotes", "quote_token", "TEXT")
    add_column_if_missing(conn, "quotes", "customer_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "vehicle_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "approved_json", "TEXT")
    add_column_if_missing(conn, "quotes", "signature_data", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_name", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_at", "TEXT")
    add_column_if_missing(conn, "quotes", "status", "TEXT DEFAULT 'quote'")
    add_column_if_missing(conn, "quotes", "sms_sent_at", "TEXT")
    add_column_if_missing(conn, "quotes", "sms_status", "TEXT")
    add_column_if_missing(conn, "quotes", "sms_sid", "TEXT")

    add_column_if_missing(conn, "invoices", "payment_status", "TEXT DEFAULT 'unpaid'")
    add_column_if_missing(conn, "invoices", "payment_method", "TEXT")
    add_column_if_missing(conn, "invoices", "paid_at", "TEXT")

    conn.commit()
    conn.close()


init_db()

def normalize_phone_number(phone):
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if (phone or "").strip().startswith("+"):
        return (phone or "").strip()
    return f"+{digits}"


def build_quote_url(token):
    if APP_BASE_URL:
        return f"{APP_BASE_URL}/quote/{token}"
    return url_for("view_quote", token=token, _external=True)


def send_quote_sms_message(phone, token, customer_name):
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN or not TWILIO_PHONE_NUMBER:
        return False, "Twilio environment variables are missing.", None

    normalized_phone = normalize_phone_number(phone)
    if not normalized_phone:
        return False, "Customer phone number is missing or invalid.", None

    quote_url = build_quote_url(token)
    display_name = (customer_name or "there").strip()
    body = (
        f"DJ's Mobile Mechanic: Hi {display_name}, here is your estimate: "
        f"{quote_url} Review and approve online."
    )

    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            body=body,
            from_=TWILIO_PHONE_NUMBER,
            to=normalized_phone,
        )
        return True, "Text sent successfully.", message.sid
    except Exception as exc:
        return False, str(exc), None


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
            normalized.append(
                {
                    "part_desc": (part.get("part_desc") or "").strip(),
                    "qty": safe_float(part.get("qty", 1), 1),
                    "oem": safe_float(part.get("oem", 0)),
                    "quality": safe_float(part.get("quality", 0)),
                    "economy": safe_float(part.get("economy", 0)),
                }
            )
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
        }]

    return []


def get_job_parts_total(job, tier="quality"):
    tier = (tier or "quality").lower()
    if tier not in ("oem", "quality", "economy"):
        tier = "quality"

    total = 0.0
    for part in normalize_job_parts(job):
        qty = safe_float(part.get("qty", 1), 1)
        price = safe_float(part.get(tier, 0))
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
            tier = (item.get("tier") or default_tier or "quality").strip().lower()
            if tier not in ("oem", "quality", "economy"):
                tier = "quality"
            selections_map[part_index] = tier

    total = 0.0
    for idx, part in enumerate(parts):
        qty = safe_float(part.get("qty", 1), 1)
        tier = selections_map.get(idx, (default_tier or "quality").lower())
        if tier not in ("oem", "quality", "economy"):
            tier = "quality"
        price = safe_float(part.get(tier, 0))
        total += qty * price

    return round(total, 2)


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


@app.route("/", methods=["GET"])
def index():
    conn = get_db()
    customers, vehicles = get_customers_and_vehicles(conn)
    conn.close()
    return render_template(
        "index.html",
        customers=customers,
        vehicles=vehicles,
        default_labor_rate=DEFAULT_LABOR_RATE,
        default_tax_rate=DEFAULT_TAX_RATE,
        default_service_fee=DEFAULT_SERVICE_FEE,
    )


@app.route("/save_quote", methods=["POST"])
def save_quote():
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

        parts = []
        part_max_len = max(
            len(part_descs),
            len(part_qtys),
            len(part_oems),
            len(part_qualities),
            len(part_economies),
        ) if any([part_descs, part_qtys, part_oems, part_qualities, part_economies]) else 0

        for p in range(part_max_len):
            part_desc = part_descs[p].strip() if p < len(part_descs) else ""
            qty = safe_float(part_qtys[p] if p < len(part_qtys) else 1, 1)
            oem = safe_float(part_oems[p] if p < len(part_oems) else 0, 0)
            quality = safe_float(part_qualities[p] if p < len(part_qualities) else 0, 0)
            economy = safe_float(part_economies[p] if p < len(part_economies) else 0, 0)

            if not part_desc and qty == 1 and oem == 0 and quality == 0 and economy == 0:
                continue

            parts.append({
                "part_desc": part_desc,
                "qty": qty,
                "oem": oem,
                "quality": quality,
                "economy": economy,
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
    token = generate_token()

    conn = get_db()
    cur = conn.cursor()
    customer_id = find_or_create_customer(conn, customer_name, customer_phone, customer_email)
    vehicle_id = find_or_create_vehicle(conn, customer_id, vehicle, vin) if customer_id else None

    cur.execute(
        """
        INSERT INTO quotes (
            quote_token, created_at, customer_id, vehicle_id, customer_name, customer_phone,
            customer_email, vehicle, vin, labor_rate, tax_rate, service_fee, payload_json, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token,
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
            "quote",
        ),
    )

    conn.commit()
    conn.close()
    return redirect(url_for("view_quote", token=token))


@app.route("/quote/<token>")
def view_quote(token):
    conn = get_db()
    row = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()

    if row is None:
        return "Quote not found", 404

    quote = dict(row)
    payload = json.loads(quote["payload_json"] or "{}")
    jobs = payload.get("jobs", [])

    _approved_jobs, approved_map = parse_approved_map(quote.get("approved_json"))
    return render_template("quote.html", quote=quote, jobs=jobs, approved_map=approved_map)


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

        for part_index, _part in enumerate(parts):
            selected_tier = request.form.get(f"part_tier_{idx}_{part_index}", "quality").strip().lower()
            if selected_tier not in ("oem", "quality", "economy"):
                selected_tier = "quality"

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


@app.route("/admin", methods=["GET"])
def admin():
    conn = get_db()
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
    rows = cur.fetchall()
    conn.close()
    return render_template("admin_quotes.html", rows=rows)


@app.route("/send_quote_sms/<token>", methods=["POST"])
def send_quote_sms_route(token):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,))
    quote = cur.fetchone()

    if not quote:
        conn.close()
        abort(404)

    phone = (quote["customer_phone"] or "").strip()
    customer_name = (quote["customer_name"] or "there").strip()

    success, message_text, sms_sid = send_quote_sms_message(phone, token, customer_name)

    if success:
        cur.execute(
            """
            UPDATE quotes
            SET sms_sent_at = ?, sms_status = ?, sms_sid = ?
            WHERE quote_token = ?
            """,
            (datetime.now().isoformat(), "sent", sms_sid, token),
        )
        conn.commit()
        flash("Quote text sent successfully.")
    else:
        cur.execute(
            """
            UPDATE quotes
            SET sms_sent_at = ?, sms_status = ?, sms_sid = ?
            WHERE quote_token = ?
            """,
            (datetime.now().isoformat(), f"failed: {message_text}", None, token),
        )
        conn.commit()
        flash(f"Text failed: {message_text}")

    conn.close()
    return redirect(url_for("admin"))


@app.route("/mark_paid/<invoice_number>", methods=["POST"])
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


if __name__ == "__main__":
    app.run(debug=True)

import os
import json
import sqlite3
import secrets
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, abort, jsonify, flash

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
app.secret_key = secrets.token_hex(16)

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


def build_quote_url_external(token):
    base = get_public_base_url()
    return f"{base}/quote/{token}" if base else f"/quote/{token}"


def build_copy_text_message(quote):
    customer_name = (quote['customer_name'] or '').strip()
    first_name = customer_name.split()[0] if customer_name else 'there'
    vehicle = (quote['vehicle'] or 'your vehicle').strip()
    quote_url = build_quote_url_external(quote['quote_token'])

    lines = [
        "DJ's Mobile Mechanic",
        '',
        f"Hi {first_name}, your quote for {vehicle} is ready:",
        quote_url,
        '',
        'Let me know if you would like to move forward or if you have any questions.',
    ]
    return '\n'.join(lines)


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

    conn.commit()

    add_column_if_missing(conn, "quotes", "quote_token", "TEXT")
    add_column_if_missing(conn, "quotes", "customer_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "vehicle_id", "INTEGER")
    add_column_if_missing(conn, "quotes", "approved_json", "TEXT")
    add_column_if_missing(conn, "quotes", "signature_data", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_name", "TEXT")
    add_column_if_missing(conn, "quotes", "signed_at", "TEXT")
    add_column_if_missing(conn, "quotes", "status", "TEXT DEFAULT 'quote'")
    add_column_if_missing(conn, "quotes", "inspection_json", "TEXT")

    add_column_if_missing(conn, "invoices", "payment_status", "TEXT DEFAULT 'unpaid'")
    add_column_if_missing(conn, "invoices", "payment_method", "TEXT")
    add_column_if_missing(conn, "invoices", "paid_at", "TEXT")

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
                "markup_oem": safe_float(part.get("markup_oem", 1.0 if oem else 1.4), 1.4),
                "markup_quality": safe_float(part.get("markup_quality", 1.0 if quality else 1.4), 1.4),
                "markup_economy": safe_float(part.get("markup_economy", 1.0 if economy else 1.4), 1.4),
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
            "markup_oem": 1.0,
            "markup_quality": 1.0,
            "markup_economy": 1.0,
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
    uploads = files.getlist("inspection_photo[]") if files else []
    items = []
    max_len = max(len(labels), len(statuses), len(notes_list), len(uploads)) if any([labels, statuses, notes_list, uploads]) else 0
    upload_dir = quote_upload_dir(token) if token else None

    for idx in range(max_len):
        label = labels[idx].strip() if idx < len(labels) else ""
        status = (statuses[idx].strip().lower() if idx < len(statuses) else "good") or "good"
        if status not in ("good", "monitor", "needs_attention"):
            status = "good"
        notes = notes_list[idx].strip() if idx < len(notes_list) else ""
        photo = ""
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


@app.route("/", methods=["GET"])
def index():
    conn = get_db()
    customers, vehicles = get_customers_and_vehicles(conn)
    all_job_presets = get_all_job_presets(conn)
    conn.close()
    return render_template(
        "index.html",
        customers=customers,
        vehicles=vehicles,
        default_labor_rate=DEFAULT_LABOR_RATE,
        default_tax_rate=DEFAULT_TAX_RATE,
        default_service_fee=DEFAULT_SERVICE_FEE,
        job_presets_json=json.dumps(all_job_presets),
    )


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
            len(part_markup_oems), len(part_markup_qualities), len(part_markup_economies),
            len(enabled_oems), len(enabled_qualities), len(enabled_economies), len(selected_tiers)
        ) if any([part_descs, part_qtys, part_oems, part_qualities, part_economies, part_list_oems, part_list_qualities, part_list_economies, part_markup_oems, part_markup_qualities, part_markup_economies, enabled_oems, enabled_qualities, enabled_economies, selected_tiers]) else 0

        for p in range(part_max_len):
            part_desc = part_descs[p].strip() if p < len(part_descs) else ""
            qty = safe_float(part_qtys[p] if p < len(part_qtys) else 1, 1)
            oem = safe_float(part_oems[p] if p < len(part_oems) else 0, 0)
            quality = safe_float(part_qualities[p] if p < len(part_qualities) else 0, 0)
            economy = safe_float(part_economies[p] if p < len(part_economies) else 0, 0)

            list_oem = safe_float(part_list_oems[p] if p < len(part_list_oems) else oem, oem)
            list_quality = safe_float(part_list_qualities[p] if p < len(part_list_qualities) else quality, quality)
            list_economy = safe_float(part_list_economies[p] if p < len(part_list_economies) else economy, economy)
            markup_oem = safe_float(part_markup_oems[p] if p < len(part_markup_oems) else (1.0 if oem else 1.4), 1.4)
            markup_quality = safe_float(part_markup_qualities[p] if p < len(part_markup_qualities) else (1.0 if quality else 1.4), 1.4)
            markup_economy = safe_float(part_markup_economies[p] if p < len(part_markup_economies) else (1.0 if economy else 1.4), 1.4)
            enabled_oem = (enabled_oems[p].strip() == "1") if p < len(enabled_oems) else (oem > 0)
            enabled_quality = (enabled_qualities[p].strip() == "1") if p < len(enabled_qualities) else (quality > 0 or (not enabled_oem and economy <= 0))
            enabled_economy = (enabled_economies[p].strip() == "1") if p < len(enabled_economies) else (economy > 0)
            selected_tier = selected_tiers[p].strip().lower() if p < len(selected_tiers) else "quality"
            selected_tier = sanitize_selected_tier({
                "enabled_oem": enabled_oem,
                "enabled_quality": enabled_quality,
                "enabled_economy": enabled_economy,
            }, selected_tier, default_tier="quality")

            if not part_desc and qty == 1 and oem == 0 and quality == 0 and economy == 0 and list_oem == 0 and list_quality == 0 and list_economy == 0:
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
    token = generate_token()
    inspection_data = build_inspection_from_request(request.form, request.files, token=token)

    conn = get_db()
    cur = conn.cursor()
    customer_id = find_or_create_customer(conn, customer_name, customer_phone, customer_email)
    vehicle_id = find_or_create_vehicle(conn, customer_id, vehicle, vin) if customer_id else None

    cur.execute(
        """
        INSERT INTO quotes (
            quote_token, created_at, customer_id, vehicle_id, customer_name, customer_phone,
            customer_email, vehicle, vin, labor_rate, tax_rate, service_fee, payload_json, inspection_json, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            json.dumps(inspection_data),
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
    jobs = [{**job, "parts": normalize_job_parts(job)} for job in jobs]

    _approved_jobs, approved_map = parse_approved_map(quote.get("approved_json"))
    inspection = load_inspection(quote)
    return render_template("quote.html", quote=quote, jobs=jobs, approved_map=approved_map, inspection=inspection)


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
    raw_rows = cur.fetchall()
    conn.close()

    rows = []
    for row in raw_rows:
        row_dict = dict(row)
        row_dict["quote_url_external"] = build_quote_url_external(row["quote_token"]) if row["quote_token"] else ""
        row_dict["customer_phone_display"] = display_phone_number(row["customer_phone"])
        row_dict["customer_phone_e164"] = normalize_phone_number(row["customer_phone"])
        rows.append(row_dict)

    return render_template("admin_quotes.html", rows=rows)


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


@app.route("/admin/copy_quote_text/<token>", methods=["GET"])
def copy_quote_text(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()
    if not quote:
        abort(404)

    return jsonify({
        "ok": True,
        "message": build_copy_text_message(quote),
        "quote_url": build_quote_url_external(token),
        "phone": normalize_phone_number(quote["customer_phone"]),
        "phone_display": display_phone_number(quote["customer_phone"]),
        "customer_name": quote["customer_name"] or "",
        "vehicle": quote["vehicle"] or "",
    })


@app.route("/send_quote_sms/<token>", methods=["POST"])
def send_quote_sms(token):
    conn = get_db()
    quote = conn.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()
    conn.close()
    if not quote:
        abort(404)
    flash("SMS sending is paused in this build. Use Copy Text to send the quote link manually.")
    return redirect(url_for("admin"))


@app.route("/admin/delete_invoice/<invoice_number>", methods=["POST"])
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


@app.route("/admin/delete_quote/<int:quote_id>", methods=["POST"])
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

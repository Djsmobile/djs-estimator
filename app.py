
from flask import Flask, render_template, request, redirect, url_for
import sqlite3, json, uuid

app = Flask(__name__)

DB = "quotes.db"

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/edit_quote/<token>")
def edit_quote(token):
    conn = get_db()
    cur = conn.cursor()

    row = cur.execute("SELECT * FROM quotes WHERE quote_token = ?", (token,)).fetchone()

    if not row:
        return "Quote not found", 404

    if row["status"] in ["approved", "invoiced"]:
        return "Locked quote", 403

    jobs = json.loads(row["jobs_json"]) if row["jobs_json"] else []

    return render_template("index.html",
        edit_mode=True,
        quote_token=token,
        existing_data=row,
        jobs=jobs
    )

@app.route("/save_quote", methods=["POST"])
def save_quote():
    conn = get_db()
    cur = conn.cursor()

    token = request.form.get("quote_token")

    customer_name = request.form.get("customer_name")
    vehicle = request.form.get("vehicle")
    jobs_json = request.form.get("jobs_json")

    if token:
        cur.execute("""
            UPDATE quotes SET
                customer_name=?,
                vehicle=?,
                jobs_json=?
            WHERE quote_token=?
        """, (customer_name, vehicle, jobs_json, token))
    else:
        token = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO quotes (quote_token, customer_name, vehicle, jobs_json)
            VALUES (?, ?, ?, ?)
        """, (token, customer_name, vehicle, jobs_json))

    conn.commit()
    return redirect(f"/quote/{token}")

@app.route("/quote/<token>")
def quote(token):
    conn = get_db()
    cur = conn.cursor()

    row = cur.execute("SELECT * FROM quotes WHERE quote_token=?", (token,)).fetchone()
    jobs = json.loads(row["jobs_json"]) if row["jobs_json"] else []

    return render_template("quote.html", jobs=jobs, row=row)

if __name__ == "__main__":
    app.run(debug=True)

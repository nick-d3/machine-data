import csv
import os
import sqlite3
from datetime import datetime, date
from io import StringIO
from pathlib import Path
from uuid import uuid4

from flask import Flask, jsonify, request, send_from_directory, Response
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DB_PATH = Path(os.getenv("DB_PATH", BASE_DIR / "data" / "slips.db"))
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", BASE_DIR / "exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

REQUIRED_FIELDS = [
    "date",
    "driver",
    "truck_number",
    "job",
    "haul_to",
    "start_time",
    "end_time",
    "material",
    "signature_name",
]


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/")

    init_db()

    @app.route("/")
    def serve_index():
        return send_from_directory(app.static_folder, "index.html")

    @app.get("/api/slips")
    def list_slips():
        limit = request.args.get("limit", default=100, type=int)
        with get_db() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM slips
                ORDER BY date DESC, created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
        return jsonify(rows)

    @app.post("/api/slips")
    def create_slip():
        data = request.get_json(silent=True) or {}

        missing = [field for field in REQUIRED_FIELDS if not data.get(field)]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        if not _valid_time_order(data.get("start_time"), data.get("end_time")):
            return jsonify({"error": "Start time must be before end time"}), 400

        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        slip_id = str(uuid4())

        slip = {
            "id": slip_id,
            "date": data.get("date"),
            "driver": data.get("driver"),
            "truck_number": data.get("truck_number"),
            "foreman": data.get("foreman") or "",
            "job": data.get("job"),
            "haul_from": data.get("haul_from") or "",
            "haul_to": data.get("haul_to"),
            "start_time": data.get("start_time"),
            "end_time": data.get("end_time"),
            "material": data.get("material"),
            "signature_name": data.get("signature_name"),
            "notes": data.get("notes") or "",
            "created_at": now,
            "updated_at": now,
        }

        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO slips (
                    id, date, driver, truck_number, foreman, job, haul_from, haul_to,
                    start_time, end_time, material, signature_name, notes, created_at, updated_at
                ) VALUES (:id, :date, :driver, :truck_number, :foreman, :job, :haul_from, :haul_to,
                          :start_time, :end_time, :material, :signature_name, :notes, :created_at, :updated_at)
                """,
                slip,
            )

        _append_csv_export(slip)

        return jsonify(slip), 201

    @app.get("/api/slips/export.csv")
    def export_csv():
        with get_db() as conn:
            cursor = conn.execute(
                "SELECT * FROM slips ORDER BY date DESC, created_at DESC"
            )
            rows = [dict(row) for row in cursor.fetchall()]

        output = _rows_to_csv(rows)
        return Response(
            output,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=slips-export.csv"},
        )

    return app


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS slips (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                driver TEXT NOT NULL,
                truck_number TEXT NOT NULL,
                foreman TEXT,
                job TEXT NOT NULL,
                haul_from TEXT,
                haul_to TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                material TEXT NOT NULL,
                signature_name TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def _valid_time_order(start: str, end: str) -> bool:
    try:
        start_dt = datetime.strptime(start, "%H:%M")
        end_dt = datetime.strptime(end, "%H:%M")
        return start_dt < end_dt
    except Exception:
        return False


def _append_csv_export(slip: dict) -> None:
    export_name = f"slips-{date.today().isoformat()}.csv"
    export_path = EXPORT_DIR / export_name

    is_new = not export_path.exists()
    fieldnames = [
        "id",
        "date",
        "driver",
        "truck_number",
        "foreman",
        "job",
        "haul_from",
        "haul_to",
        "start_time",
        "end_time",
        "material",
        "signature_name",
        "notes",
        "created_at",
        "updated_at",
    ]

    with export_path.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        writer.writerow({key: slip.get(key, "") for key in fieldnames})


def _rows_to_csv(rows: list[dict]) -> str:
    fieldnames = [
        "id",
        "date",
        "driver",
        "truck_number",
        "foreman",
        "job",
        "haul_from",
        "haul_to",
        "start_time",
        "end_time",
        "material",
        "signature_name",
        "notes",
        "created_at",
        "updated_at",
    ]

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return output.getvalue()


if __name__ == "__main__":
    app = create_app()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

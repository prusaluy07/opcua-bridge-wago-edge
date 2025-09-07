import os
import requests
import sqlite3
import time
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# Konfiguration (ENV Variablen)
# -----------------------------
ANYTHINGLLM_URL = os.getenv("ANYTHINGLLM_URL", "http://anythingllm:3001")
API_KEY = os.getenv("ANYTHINGLLM_API_KEY", "changeme")
WORKSPACE = os.getenv("ANYTHINGLLM_WORKSPACE", "default")

DB_FILE = "data/errors.db"

# -----------------------------
# DB Setup
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            code TEXT,
            description TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def already_exists(machine, code, description):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM errors 
        WHERE machine=? AND code=? AND description=? 
        AND DATE(timestamp)=DATE('now')
    """, (machine, code, description))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

def save_error(machine, code, description):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO errors (machine, code, description, timestamp)
        VALUES (?, ?, ?, ?)
    """, (machine, code, description, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# -----------------------------
# AnythingLLM API
# -----------------------------
def send_to_documents(machine, code, description):
    url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE}/documents"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    doc = {
        "title": f"{machine} Fehler {code}",
        "content": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "tags": [machine, "fehler", code]
    }
    try:
        r = requests.post(url, headers=headers, json=doc, timeout=10)
        logging.info("Doc upload: %s (%s)", r.status_code, doc["title"])
    except Exception as e:
        logging.error("Fehler beim Senden an Documents: %s", e)

def send_to_chat(machine, code, description):
    url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE}/chat"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    message = {
        "message": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "conversation": f"{machine}-errors"
    }
    try:
        r = requests.post(url, headers=headers, json=message, timeout=10)
        logging.info("Chat message: %s (%s)", r.status_code, message["conversation"])
    except Exception as e:
        logging.error("Fehler beim Senden an Chat: %s", e)

# -----------------------------
# Health Check
# -----------------------------
def check_api_health():
    try:
        r = requests.get(f"{ANYTHINGLLM_URL}/api/v1/health", timeout=5)
        if r.status_code == 200:
            logging.info("✅ AnythingLLM API erreichbar")
        else:
            logging.warning("⚠️ AnythingLLM API antwortet mit Status: %s", r.status_code)
    except Exception as e:
        logging.error("❌ API nicht erreichbar: %s", e)

# -----------------------------
# Simulierter OPC/AnyViz Input
# -----------------------------
def get_new_errors():
    # Später echte OPC UA / AnyViz Integration
    return [
        ("Station-3", "4711", "Hydraulikdruck zu niedrig"),
        ("Station-5", "1023", "Not-Aus ausgelöst"),
    ]

# -----------------------------
# Main Loop
# -----------------------------
def main():
    init_db()
    check_api_health()
    logging.info("🚀 OPC-UA Bridge gestartet. Warte auf Meldungen...")

    executor = ThreadPoolExecutor(max_workers=4)

    while True:
        errors = get_new_errors()
        for machine, code, description in errors:
            if not already_exists(machine, code, description):
                save_error(machine, code, description)
                # Parallel senden
                executor.submit(send_to_documents, machine, code, description)
                executor.submit(send_to_chat, machine, code, description)
            else:
                logging.info("⚠️ Fehler %s von %s heute schon gespeichert – übersprungen.", code, machine)

        time.sleep(60)

if __name__ == "__main__":
    main()

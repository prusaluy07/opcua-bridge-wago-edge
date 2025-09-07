import os
import requests
import sqlite3
import time
import random
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
# Konfiguration
# -----------------------------
ANYTHINGLLM_URL = os.getenv("ANYTHINGLLM_URL", "http://anythingllm:3001")
API_KEY = os.getenv("ANYTHINGLLM_API_KEY", "KE7053N-30JM5PZ-KAPMXDP-KXJQC3N")
WORKSPACE = os.getenv("ANYTHINGLLM_WORKSPACE", "wago-edge-copilot")

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
        "tags": ["Fehler", machine, code]  # einfache Tags, sichtbar
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
        "conversation": "general"  # sichtbar in Haupt-Thread
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
# Fehlergenerator
# -----------------------------
MACHINES = ["Station-1", "Station-2", "Station-3", "Station-4", "Station-5"]
ERROR_CODES = [
    ("1001", "Hydraulikdruck zu niedrig"),
    ("1002", "Öldruck niedrig"),
    ("1003", "Not-Aus ausgelöst"),
    ("1004", "Temperatur zu hoch"),
    ("1005", "Sensorfehler")
]

def generate_new_error():
    """Generiert einen Fehler, der heute noch nicht existiert"""
    while True:
        machine = random.choice(MACHINES)
        code, description = random.choice(ERROR_CODES)
        if not already_exists(machine, code, description):
            return machine, code, description

# -----------------------------
# Main Loop
# -----------------------------
def main():
    init_db()
    check_api_health()
    logging.info("🚀 OPC-UA Bridge gestartet. Warte auf Meldungen...")

    executor = ThreadPoolExecutor(max_workers=4)

    while True:
        machine, code, description = generate_new_error()
        save_error(machine, code, description)
        executor.submit(send_to_documents, machine, code, description)
        executor.submit(send_to_chat, machine, code, description)
        logging.info("🆕 Neuer Fehler generiert: %s %s - %s", machine, code, description)
        time.sleep(60)  # alle 60 Sekunden ein neuer Fehler

if __name__ == "__main__":
    main()

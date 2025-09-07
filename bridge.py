import os
import sqlite3
import time
import random
from datetime import datetime
import requests

# -----------------------------
# Konfiguration
# -----------------------------
ANYTHINGLLM_IP = os.getenv("ANYTHINGLLM_IP", "172.20.0.2")  # interne Container-IP
ANYTHINGLLM_PORT = os.getenv("ANYTHINGLLM_PORT", "3001")
API_KEY = os.getenv("ANYTHINGLLM_API_KEY", "KE7053N-30JM5PZ-KAPMXDP-KXJQC3N")
WORKSPACE = os.getenv("ANYTHINGLLM_WORKSPACE", "wago-edge-copilot")
CHAT_THREAD = os.getenv("ANYTHINGLLM_THREAD", "0c4ae3fa-a7ff-4c1d-b240-6f9ef4327a01")

DB_FILE = "data/errors.db"

BASE_URL = f"http://{ANYTHINGLLM_IP}:{ANYTHINGLLM_PORT}/api/v1/workspaces/{WORKSPACE}"

# -----------------------------
# SQLite Setup
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            machine TEXT,
            description TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def already_exists(code, machine, description):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM errors 
        WHERE code=? AND machine=? AND description=?
        AND DATE(timestamp)=DATE('now')
    """, (code, machine, description))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

def save_error(code, machine, description):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO errors (code, machine, description, timestamp)
        VALUES (?, ?, ?, ?)
    """, (code, machine, description, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# -----------------------------
# AnythingLLM API
# -----------------------------
def send_to_chat(machine, code, description):
    url = f"{BASE_URL}/chat"
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    payload = {
        "message": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "conversation": CHAT_THREAD
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.headers.get("Content-Type", "").startswith("application/json"):
            print("Chat:", r.status_code, r.json())
        else:
            print("Chat:", r.status_code, "HTML erhalten, Nachricht gesendet?")
    except Exception as e:
        print("Fehler beim Senden an Chat:", e)

def send_to_documents(machine, code, description):
    url = f"{BASE_URL}/documents"
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    payload = {
        "title": f"{machine} Fehler {code}",
        "content": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "tags": [machine, "fehler", code]
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.headers.get("Content-Type", "").startswith("application/json"):
            print("Doc:", r.status_code, r.json())
        else:
            print("Doc:", r.status_code, "HTML erhalten, Dokument evtl. erstellt?")
    except Exception as e:
        print("Fehler beim Senden an Documents:", e)

# -----------------------------
# Neue Fehler simulieren
# -----------------------------
MACHINES = ["Station-1", "Station-2", "Station-3", "Station-4"]
ERROR_CODES = ["1001", "1002", "4711", "1023"]
ERROR_DESC = [
    "Hydraulikdruck zu niedrig",
    "Not-Aus ausgelöst",
    "Sensor defekt",
    "Temperatur überschritten"
]

def generate_error():
    machine = random.choice(MACHINES)
    code = random.choice(ERROR_CODES)
    description = random.choice(ERROR_DESC)
    return machine, code, description

# -----------------------------
# Main Loop
# -----------------------------
def main():
    init_db()
    print("🚀 OPC-UA Bridge mit AnythingLLM gestartet...")

    while True:
        machine, code, description = generate_error()
        if not already_exists(code, machine, description):
            save_error(code, machine, description)
            send_to_documents(machine, code, description)
            send_to_chat(machine, code, description)
        else:
            print(f"⚠️ Fehler {code} von {machine} heute schon gespeichert – übersprungen.")
        time.sleep(60)  # alle 60 Sekunden

if __name__ == "__main__":
    main()

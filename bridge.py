import os
import requests
import sqlite3
from datetime import datetime

# -----------------------------
# Konfiguration (ENV Variablen)
# -----------------------------
ANYTHINGLLM_URL = os.getenv("ANYTHINGLLM_URL", "http://anythingllm:3001")
API_KEY = os.getenv("ANYTHINGLLM_API_KEY", "changeme")
WORKSPACE = os.getenv("ANYTHINGLLM_WORKSPACE", "default")

# SQLite DB
DB_FILE = "errors.db"

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
        SELECT 1 FROM errors WHERE machine=? AND code=? AND description=? AND DATE(timestamp)=DATE('now')
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
# AnythingLLM Funktionen
# -----------------------------
def send_to_documents(machine, code, description):
    url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE}/documents"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    doc = {
        "title": f"{machine} Fehler {code}",
        "content": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "tags": [machine, "fehler", code]
    }
    r = requests.post(url, headers=headers, json=doc)
    print("Doc upload:", r.status_code, r.text)

def send_to_chat(machine, code, description):
    url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE}/chat"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    message = {
        "message": f"{datetime.now().isoformat()} - {machine} meldet Fehler {code}: {description}",
        "conversation": f"{machine}-errors"
    }
    r = requests.post(url, headers=headers, json=message)
    print("Chat message:", r.status_code, r.text)

# -----------------------------
# Simulierter OPC/AnyViz Input
# -----------------------------
def get_new_errors():
    # Hier später deine AnyViz/OPC UA Anbindung
    # Jetzt nur Demo-Daten:
    return [
        ("Station-3", "4711", "Hydraulikdruck zu niedrig"),
        ("Station-5", "1023", "Not-Aus ausgelöst"),
    ]

# -----------------------------
# Main Loop
# -----------------------------
def main():
    init_db()
    errors = get_new_errors()
    for machine, code, description in errors:
        if not already_exists(machine, code, description):
            save_error(machine, code, description)
            send_to_documents(machine, code, description)
            send_to_chat(machine, code, description)
        else:
            print(f"Fehler {code} von {machine} bereits gespeichert – übersprungen.")

if __name__ == "__main__":
    main()

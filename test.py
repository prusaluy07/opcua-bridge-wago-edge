#!/usr/bin/env python3
import requests

# Variablen anpassen
ANYTHINGLLM_URL = "http://anythingllm:3001"
API_KEY = "KE7053N-30JM5PZ-KAPMXDP-KXJQC3N"
WORKSPACE_SLUG = "wago-edge-copilot"
MESSAGE = "🚨 Live-Testfehler"

# 1. Workspace abrufen
workspace_url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE_SLUG}"
headers = {"Authorization": f"Bearer {API_KEY}"}

resp = requests.get(workspace_url, headers=headers)
resp.raise_for_status()
workspace_data = resp.json()

# 2. Thread auswählen (z.B. der erste vorhandene Thread)
threads = workspace_data.get("threads", [])
if not threads:
    raise ValueError("Keine Threads im Workspace gefunden!")

thread_slug = threads[0]["slug"]
print(f"Verwende Thread: {threads[0]['name']} ({thread_slug})")

# 3. Nachricht senden
chat_url = f"{ANYTHINGLLM_URL}/api/v1/workspaces/{WORKSPACE_SLUG}/chat"
payload = {
    "message": MESSAGE,
    "conversation": thread_slug
}

resp = requests.post(chat_url, headers={**headers, "Content-Type": "application/json"}, json=payload)
resp.raise_for_status()

# 4. Ausgabe
print("Status Code:", resp.status_code)
print("Antwort:", resp.json())

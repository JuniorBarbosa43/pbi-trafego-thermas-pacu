"""
CARGA HISTORICA WhatsApp -> Google Sheets
==========================================
Busca todos os analytics de templates WhatsApp desde 15/02/2026 ate hoje,
consultando a API GHL semana a semana (a API limita o range por chamada).

Como rodar:
  python scripts/carga_historica_whatsapp.py
  python scripts/carga_historica_whatsapp.py --start 2026-02-15 --end 2026-04-12
"""

import os
import sys
import json
import datetime
import argparse
import urllib.request
import urllib.parse
import urllib.error
import time

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, limpar_e_gravar, criar_sheet_se_nao_existe

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2026-02-15", help="Data inicio (YYYY-MM-DD)")
parser.add_argument("--end",   default=datetime.date.today().isoformat(), help="Data fim (YYYY-MM-DD)")
args = parser.parse_args()

START_DATE = datetime.date.fromisoformat(args.start)
END_DATE   = datetime.date.fromisoformat(args.end)

# Credenciais
FIREBASE_REFRESH_TOKEN = os.environ["GHL_FIREBASE_REFRESH_TOKEN"]
FIREBASE_API_KEY       = os.environ["GHL_FIREBASE_API_KEY"]
LOCATION_ID  = os.environ["GHL_LOCATION"]
WABA_ID      = os.environ["GHL_WABA_ID"]

def obter_ghl_token() -> str:
    """Usa Firebase refresh token para obter novo GHL session JWT."""
    fb_data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": FIREBASE_REFRESH_TOKEN,
    }).encode("utf-8")
    fb_req = urllib.request.Request(
        f"https://securetoken.googleapis.com/v1/token?key={FIREBASE_API_KEY}",
        data=fb_data, method="POST"
    )
    fb_req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(fb_req, timeout=30) as resp:
        id_token = json.loads(resp.read())["id_token"]
    print("  Firebase ID token obtido")

    ghl_req = urllib.request.Request(
        "https://backend.leadconnectorhq.com/user/login",
        data=json.dumps({"token": id_token}).encode("utf-8"),
        method="POST"
    )
    ghl_req.add_header("Content-Type", "application/json")
    ghl_req.add_header("Version", "2021-07-28")
    with urllib.request.urlopen(ghl_req, timeout=30) as resp:
        ghl_resp = json.loads(resp.read())
    token = ghl_resp.get("token") or ghl_resp.get("access_token") or ghl_resp.get("jwt")
    if not token:
        raise RuntimeError(f"GHL login sem token. Resp: {str(ghl_resp)[:200]}")
    print("  GHL JWT obtido")
    return token

print("Obtendo GHL token via Firebase...")
GHL_TOKEN = obter_ghl_token()

GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID       = os.environ["SPREADSHEET_ID"]

BASE_URL = "https://backend.leadconnectorhq.com"

def infer_departamento(name: str) -> str:
    n = (name or "").lower()
    if any(x in n for x in ("cobranca","cobran","parcela","renegoc","reativacao","carteirinha")):
        return "Cobranca"
    if "hosped" in n: return "Hospedagem"
    if any(x in n for x in ("excurs","escola")): return "Excursoes"
    if "sdr" in n: return "SDR"
    if "agendamento" in n: return "Comercial"
    if any(x in n for x in ("dayuse","marketing","volta10")): return "Marketing"
    if any(x in n for x in ("card","venda","migracao","vip")): return "Vendas"
    return "Nao Classificado"

def ghl_get(url: str) -> dict:
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {GHL_TOKEN}")
    req.add_header("Version", "2021-07-28")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} em {url}: {body[:200]}") from e

def ts_to_iso(ts) -> str:
    if ts is None: return ""
    try: return datetime.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%dT%H:%M:%S")
    except: return str(ts)

def weeks_between(start: datetime.date, end: datetime.date):
    current = start
    while current <= end:
        week_end = min(current + datetime.timedelta(days=6), end)
        yield current, week_end
        current = week_end + datetime.timedelta(days=1)

def fetch_templates():
    url = f"{BASE_URL}/phone-system/whatsapp/location/{LOCATION_ID}/template"
    data = ghl_get(url)
    if isinstance(data, list): return data
    return data.get("templates", data.get("data", []))

def fetch_analytics(template_id: str, start_str: str, end_str: str) -> dict:
    params = (
        f"startDate={start_str}&endDate={end_str}"
        f"&wabaId={WABA_ID}&templateIds[]={template_id}"
    )
    url = f"{BASE_URL}/phone-system/whatsapp/location/{LOCATION_ID}/analytics?{params}"
    try:
        return ghl_get(url)
    except Exception as e:
        print(f"      Aviso analytics {template_id} [{start_str}->{end_str}]: {e}")
        return {}

def main():
    print("=== CARGA HISTORICA WHATSAPP ===")
    print(f"Periodo: {START_DATE} -> {END_DATE}")
    semanas = list(weeks_between(START_DATE, END_DATE))
    print(f"Semanas: {len(semanas)}")

    print("Buscando templates...")
    templates_raw = fetch_templates()
    print(f"  {len(templates_raw)} templates encontrados")

    templates_flat = []
    for tpl in templates_raw:
        templates_flat.append({
            "templateId":           str(tpl.get("id", "")),
            "templateName":         tpl.get("name", ""),
            "category":             tpl.get("category", ""),
            "language":             tpl.get("language", ""),
            "status":               tpl.get("status", ""),
            "folderId":             tpl.get("folderId") or "",
            "locationId":           tpl.get("locationId") or "",
            "createdAt":            tpl.get("createdAt") or "",
            "updatedAt":            tpl.get("updatedAt") or "",
            "departamentoInferido": infer_departamento(tpl.get("name", "")),
        })

    print(f"\nBuscando analytics ({len(semanas)} semanas x {len(templates_flat)} templates)...")
    all_analytics = []
    total_calls   = 0

    for s_start, s_end in semanas:
        s_str = s_start.isoformat()
        e_str = s_end.isoformat()
        print(f"  Semana {s_str} -> {e_str}")

        for tpl in templates_flat:
            tid  = tpl["templateId"]
            analytics = fetch_analytics(tid, s_str, e_str)
            total_calls += 1
            time.sleep(0.3)

            delivery = (analytics.get("deliverygraphData") or {}).get(tid, [])
            base = {
                "templateId":           tid,
                "templateName":         tpl["templateName"],
                "category":             tpl["category"],
                "language":             tpl["language"],
                "status":               tpl["status"],
                "folderId":             tpl["folderId"],
                "folderName":           "",
                "departamentoInferido": tpl["departamentoInferido"],
                "sentTotal":            str(analytics.get("sent") or ""),
                "deliveredTotal":       str(analytics.get("delivered") or ""),
                "readTotal":            str(analytics.get("read") or ""),
            }

            if not delivery:
                if analytics.get("sent") or analytics.get("delivered") or analytics.get("read"):
                    all_analytics.append({
                        **base,
                        "sent":      str(analytics.get("sent") or ""),
                        "delivered": str(analytics.get("delivered") or ""),
                        "read":      str(analytics.get("read") or ""),
                        "startTime": s_str + "T00:00:00",
                        "endTime":   e_str + "T23:59:59",
                        "error":     "",
                    })
            else:
                for point in delivery:
                    sent  = point.get("sent")
                    deliv = point.get("delivered")
                    rd    = point.get("read")
                    if not sent and not deliv and not rd:
                        continue
                    all_analytics.append({
                        **base,
                        "sent":      str(sent or ""),
                        "delivered": str(deliv or ""),
                        "read":      str(rd or ""),
                        "startTime": ts_to_iso(point.get("startTime")),
                        "endTime":   ts_to_iso(point.get("endTime")),
                        "error":     "",
                    })

    print(f"\nTotal chamadas API: {total_calls}")
    print(f"Total registros analytics: {len(all_analytics)}")

    print("\nAutenticando Google...")
    token = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)

    for sheet_name in ("WA_Analytics", "WA_Templates"):
        criar_sheet_se_nao_existe(SPREADSHEET_ID, sheet_name, token)

    wa_headers = [
        "templateId","templateName","category","language","status",
        "folderId","folderName","departamentoInferido",
        "sent","delivered","read",
        "sentTotal","deliveredTotal","readTotal",
        "startTime","endTime","error",
    ]
    wa_rows = [[r.get(c,"") for c in wa_headers] for r in all_analytics]
    limpar_e_gravar(SPREADSHEET_ID, "WA_Analytics", wa_headers, wa_rows, token)

    tpl_headers = [
        "templateId","templateName","category","language","status",
        "folderId","locationId","createdAt","updatedAt","departamentoInferido",
    ]
    tpl_rows = [[r.get(c,"") for c in tpl_headers] for r in templates_flat]
    limpar_e_gravar(SPREADSHEET_ID, "WA_Templates", tpl_headers, tpl_rows, token)

    print("\nCarga historica concluida!")
    print(f"   WA_Analytics: {len(all_analytics)} linhas")
    print(f"   WA_Templates: {len(templates_flat)} linhas")


if __name__ == "__main__":
    main()

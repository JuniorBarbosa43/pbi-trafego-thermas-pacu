"""
CARGA HISTORICA WhatsApp -> Google Sheets
=========================================
Busca todos os analytics de templates WhatsApp desde 15/02/2026 ate hoje,
consultando a API GHL semana a semana (a API limita o range por chamada).

Variaveis de ambiente necessarias:
  GHL_FIREBASE_REFRESH_TOKEN -- Firebase refresh token (longa duracao)
  GHL_FIREBASE_API_KEY       -- Firebase Web API key do app GHL
  GHL_LOCATION               -- Location ID da conta GHL
  GHL_WABA_ID                -- WABA ID (WhatsApp Business Account ID)
  GOOGLE_CLIENT_ID           -- OAuth2 client ID
  GOOGLE_CLIENT_SECRET       -- OAuth2 client secret
  GOOGLE_REFRESH_TOKEN       -- OAuth2 refresh token
  SPREADSHEET_ID             -- ID da planilha Google Sheets de destino
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
from sheets_helper import normalizar_secret, obter_access_token, limpar_e_gravar, criar_sheet_se_nao_existe

parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2026-02-15", help="Data inicio (YYYY-MM-DD)")
parser.add_argument("--end",   default=datetime.date.today().isoformat(), help="Data fim (YYYY-MM-DD)")
args = parser.parse_args()

START_DATE = datetime.date.fromisoformat(args.start)
END_DATE   = datetime.date.fromisoformat(args.end)

FIREBASE_REFRESH_TOKEN = normalizar_secret(os.environ["GHL_FIREBASE_REFRESH_TOKEN"])
FIREBASE_API_KEY       = normalizar_secret(os.environ["GHL_FIREBASE_API_KEY"])
LOCATION_ID  = normalizar_secret(os.environ["GHL_LOCATION"])
WABA_ID      = normalizar_secret(os.environ["GHL_WABA_ID"])

def obter_ghl_token():
    fb_data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": FIREBASE_REFRESH_TOKEN,
    }).encode("utf-8")
    fb_req = urllib.request.Request(
        f"https://securetoken.googleapis.com/v1/token?key={FIREBASE_API_KEY}",
        data=fb_data,
        method="POST"
    )
    fb_req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(fb_req, timeout=30) as resp:
        fb_resp = json.loads(resp.read())
    id_token = fb_resp["id_token"]
    print("  Firebase ID token obtido")

    ghl_req = urllib.request.Request(
        "https://backend.leadconnectorhq.com/user/login",
        data=json.dumps({"token": id_token}).encode("utf-8"),
        method="POST"
    )
    ghl_req.add_header("Content-Type", "application/json")
    ghl_req.add_header("Version", "2021-07-28")
    ghl_req.add_header("token-id", id_token)
    ghl_req.add_header("source", "WEB_USER")
    ghl_req.add_header("channel", "APP")
    ghl_req.add_header("Origin", "https://go.movatalks.com")
    ghl_req.add_header("Referer", "https://go.movatalks.com/")
    ghl_req.add_header("app-name", "spm-ts")
    ghl_req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

    try:
        with urllib.request.urlopen(ghl_req, timeout=30) as resp:
            ghl_resp = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  AVISO: /user/login HTTP {e.code}: {body[:300]}")
        print("  Fallback: usando Firebase ID token como Bearer...")
        return id_token, id_token

    token = ghl_resp.get("token") or ghl_resp.get("access_token") or ghl_resp.get("jwt")
    if not token:
        print(f"  AVISO: login sem token. Resposta: {str(ghl_resp)[:200]}")
        return id_token, id_token
    print("  GHL JWT obtido com sucesso")
    return token, id_token


print("Obtendo GHL token via Firebase...")
GHL_TOKEN, FIREBASE_ID_TOKEN = obter_ghl_token()

GOOGLE_CLIENT_ID     = normalizar_secret(os.environ["GOOGLE_CLIENT_ID"])
GOOGLE_CLIENT_SECRET = normalizar_secret(os.environ["GOOGLE_CLIENT_SECRET"])
GOOGLE_REFRESH_TOKEN = normalizar_secret(os.environ["GOOGLE_REFRESH_TOKEN"])
SPREADSHEET_ID       = normalizar_secret(os.environ["SPREADSHEET_ID"])

BASE_URL = "https://backend.leadconnectorhq.com"

def infer_departamento(name: str) -> str:
    n = (name or "").lower()
    if "cobranca" in n or "cobran" in n or "parcela" in n or "renegoc" in n or "reativacao" in n or "carteirinha" in n:
        return "Cobranca"
    if "hosped" in n:
        return "Hospedagem"
    if "excurs" in n or "escola" in n:
        return "Excursoes"
    if "sdr" in n:
        return "SDR"
    if "agendamento" in n:
        return "Comercial"
    if "dayuse" in n or "marketing" in n or "volta10" in n:
        return "Marketing"
    if "card" in n or "venda" in n or "migracao" in n or "vip" in n:
        return "Vendas"
    return "Nao Classificado"

def ghl_get(url: str) -> dict:
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {GHL_TOKEN}")
    req.add_header("token-id", FIREBASE_ID_TOKEN)
    req.add_header("source", "WEB_USER")
    req.add_header("channel", "APP")
    req.add_header("Version", "2021-07-28")
    req.add_header("Accept", "application/json, text/plain, */*")
    req.add_header("Origin", "https://go.movatalks.com")
    req.add_header("Referer", "https://go.movatalks.com/")
    req.add_header("app-name", "spm-ts")
    req.add_header("route-name", "whatsapp-v1")
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} em {url}: {body}") from e

def weeks_between(start: datetime.date, end: datetime.date):
    cur = start
    while cur <= end:
        week_end = min(cur + datetime.timedelta(days=6), end)
        yield cur, week_end
        cur = week_end + datetime.timedelta(days=1)

def fetch_templates() -> list:
    url = f"{BASE_URL}/phone-system/whatsapp/location/{LOCATION_ID}/template"
    data = ghl_get(url)
    if isinstance(data, list):
        return data
    return data.get("templates", data.get("data", []))

def fetch_analytics_week(template_id: str, week_start: datetime.date, week_end: datetime.date) -> dict:
    start_ts = int(datetime.datetime.combine(week_start, datetime.time.min).timestamp())
    end_ts   = int(datetime.datetime.combine(week_end, datetime.time.max).timestamp())
    params = (
        f"startDate={start_ts}&endDate={end_ts}"
        f"&wabaId={WABA_ID}&templateIds[]={template_id}"
    )
    url = f"{BASE_URL}/phone-system/whatsapp/location/{LOCATION_ID}/analytics?{params}"
    try:
        return ghl_get(url)
    except Exception as e:
        print(f"      Aviso: erro {e}")
        return {}

def ts_to_iso(ts) -> str:
    if ts is None:
        return ""
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return str(ts)

def main():
    print(f"Carga historica: {START_DATE} -> {END_DATE}")

    print("Buscando templates...")
    templates_raw = fetch_templates()
    print(f"  Templates encontrados: {len(templates_raw)}")

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

    print("Buscando analytics semana a semana...")
    analytics_flat = []
    weeks = list(weeks_between(START_DATE, END_DATE))
    print(f"  Total de semanas: {len(weeks)}")

    for tpl in templates_flat:
        tid  = tpl["templateId"]
        name = tpl["templateName"]
        print(f"  Template [{name}] id={tid}")

        for ws, we in weeks:
            print(f"    Semana {ws} -> {we}")
            analytics = fetch_analytics_week(tid, ws, we)
            delivery  = []
            if analytics:
                delivery = (analytics.get("deliverygraphData") or {}).get(tid, [])

            base_row = {
                "templateId":           tid,
                "templateName":         name,
                "category":             tpl["category"],
                "language":             tpl["language"],
                "status":               tpl["status"],
                "folderId":             tpl["folderId"],
                "folderName":           "",
                "departamentoInferido": tpl["departamentoInferido"],
                "weekStart":            ws.isoformat(),
                "weekEnd":              we.isoformat(),
                "sentTotal":            str(analytics.get("sent") or ""),
                "deliveredTotal":       str(analytics.get("delivered") or ""),
                "readTotal":            str(analytics.get("read") or ""),
            }

            if not delivery:
                analytics_flat.append({
                    **base_row,
                    "sent":      str(analytics.get("sent") or ""),
                    "delivered": str(analytics.get("delivered") or ""),
                    "read":      str(analytics.get("read") or ""),
                    "startTime": "",
                    "endTime":   "",
                    "error":     "" if analytics else "no_data",
                })
            else:
                for point in delivery:
                    analytics_flat.append({
                        **base_row,
                        "sent":      str(point.get("sent") or ""),
                        "delivered": str(point.get("delivered") or ""),
                        "read":      str(point.get("read") or ""),
                        "startTime": ts_to_iso(point.get("startTime")),
                        "endTime":   ts_to_iso(point.get("endTime")),
                        "error":     "",
                    })
            time.sleep(0.3)

    print(f"\nTotal registros analytics historicos: {len(analytics_flat)}")

    print("\nAutenticando Google...")
    token = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)

    for sheet_name in ("WA_Analytics_Historico", "WA_Templates"):
        criar_sheet_se_nao_existe(SPREADSHEET_ID, sheet_name, token)

    wa_headers = [
        "templateId", "templateName", "category", "language", "status",
        "folderId", "folderName", "departamentoInferido",
        "weekStart", "weekEnd",
        "sent", "delivered", "read",
        "sentTotal", "deliveredTotal", "readTotal",
        "startTime", "endTime", "error",
    ]
    wa_rows = [[r.get(c, "") for c in wa_headers] for r in analytics_flat]
    limpar_e_gravar(SPREADSHEET_ID, "WA_Analytics_Historico", wa_headers, wa_rows, token)

    tpl_headers = [
        "templateId", "templateName", "category", "language", "status",
        "folderId", "locationId", "createdAt", "updatedAt", "departamentoInferido",
    ]
    tpl_rows = [[r.get(c, "") for c in tpl_headers] for r in templates_flat]
    limpar_e_gravar(SPREADSHEET_ID, "WA_Templates", tpl_headers, tpl_rows, token)

    print("\nCarga historica concluida com sucesso.")


if __name__ == "__main__":
    main()

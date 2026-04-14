"""
Atualiza Meta Ads campanhas no Google Sheets via Marketing API.
Roda via GitHub Actions todo dia as 06:00.
Janela de 14 dias (cobre delay de consolidacao da API).
Modo UPSERT com opcao --historico para carga de dados passados.
"""

import json
import os
import sys
import argparse
import urllib.request
import urllib.parse
from datetime import date, timedelta
import time

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, upsert_por_data, criar_sheet_se_nao_existe

# ── Credenciais via GitHub Secrets ─────────────────────────
META_TOKEN        = os.environ["META_TOKEN"]
_raw_ad_account   = os.environ["META_AD_ACCOUNT_ID"]   # pode ser com ou sem prefixo act_
META_AD_ACCOUNT   = _raw_ad_account if _raw_ad_account.startswith("act_") else f"act_{_raw_ad_account}"
GOOGLE_CLIENT_ID  = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID    = os.environ["SPREADSHEET_ID"]
# ───────────────────────────────────────────────────────────

SHEET_NAME   = "Meta_Ads_Campanhas"
JANELA_DIAS  = 14
FALLBACK_DIAS = 365

FIELDS = "campaign_id,campaign_name,date_start,date_stop,impressions,reach,clicks,spend,ctr"
HEADERS = ["campaign_id", "campaign_name", "date_start", "date_stop",
           "impressions", "reach", "clicks", "spend", "ctr"]


def buscar_paginas(since: str, until: str) -> list:
    """Busca todos os dados paginados da API usando o link 'next' da paginacao."""
    base_query = {
        "fields":         FIELDS,
        "level":          "campaign",
        "time_increment": "1",
        "time_range":     json.dumps({"since": since, "until": until}),
        "limit":          "500",
        "access_token":   META_TOKEN,
    }
    url_base = f"https://graph.facebook.com/v25.0/{META_AD_ACCOUNT}/insights"
    registros = []
    url = url_base + "?" + urllib.parse.urlencode(base_query)

    pagina = 0
    while url:
        pagina += 1
        try:
            with urllib.request.urlopen(url) as resp:
                data = json.loads(resp.read())

            page_data = data.get("data", [])
            registros.extend(page_data)
            print(f"    Pagina {pagina}: {len(page_data)} registros")

            # Usar o link 'next' para paginacao (mais confiavel que cursors)
            next_url = data.get("paging", {}).get("next")
            if not page_data:
                print(f"    Sem mais dados. Paging: {data.get('paging', {})}")
            if next_url and page_data:
                url = next_url
                time.sleep(0.2)
            else:
                url = None
        except Exception as e:
            print(f"  ERRO ao buscar pagina {pagina}: {e}")
            url = None

    return registros


def main():
    parser = argparse.ArgumentParser(description="Atualiza Meta Ads no Google Sheets")
    parser.add_argument("--historico", action="store_true", help="Fetch dados historicos desde 2025-01-01")
    args = parser.parse_args()

    print("=== Meta Ads → Google Sheets ===")

    token = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    criar_sheet_se_nao_existe(SPREADSHEET_ID, SHEET_NAME, token)

    if args.historico:
        print("Modo HISTORICO: fetchando dados desde 2025-01-01 em chunks de 30 dias")
        inicio = date(2025, 1, 1)
        fim = date.today()
        chunk_dias = 30
        todos_registros = []

        current = inicio
        while current < fim:
            chunk_end = min(current + timedelta(days=chunk_dias), fim)
            since = current.isoformat()
            until = chunk_end.isoformat()
            print(f"  Periodo: {since} → {until}")

            registros = buscar_paginas(since, until)
            todos_registros.extend(registros)
            print(f"    Registros neste chunk: {len(registros)}")

            current = chunk_end + timedelta(days=1)
            time.sleep(0.3)

        registros = todos_registros
        print(f"Total registros historicos obtidos: {len(registros)}")
    else:
        # Modo incremental: 14 dias
        until = (date.today() - timedelta(days=1)).isoformat()
        since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
        print(f"Modo INCREMENTAL: Periodo: {since} → {until}")
        registros = buscar_paginas(since, until)
        print(f"Registros obtidos: {len(registros)}")

    # Converte para linhas
    rows = []
    for r in registros:
        rows.append([
            r.get("campaign_id", ""),
            r.get("campaign_name", ""),
            r.get("date_start", ""),
            r.get("date_stop", ""),
            int(r.get("impressions", 0)),
            int(r.get("reach", 0)),
            int(r.get("clicks", 0)),
            round(float(r.get("spend", 0)), 2),
            round(float(r.get("ctr", 0)), 4),
        ])

    # Upsert por date_start
    upsert_por_data(SPREADSHEET_ID, SHEET_NAME, HEADERS, rows, token, key_cols=["date_start"])

    print("Concluido.")


if __name__ == "__main__":
    main()

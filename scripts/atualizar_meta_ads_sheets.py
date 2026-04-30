""" Atualiza Meta Ads campanhas no Google Sheets via Marketing API.
Roda via GitHub Actions todo dia as 06:00.
Janela de 14 dias (cobre delay de consolidacao da API).
Modo UPSERT com opcao --historico para carga de dados passados.

Campos adicionados (2026-04):
  - purchases: acoes do tipo 'purchase' reportadas pelo Meta
  - purchase_value: receita atribuida pelo Meta (em BRL)
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

# ── Credenciais via GitHub Secrets ────────────────────────
META_TOKEN = os.environ["META_TOKEN"]
_raw_ad_account = os.environ["META_AD_ACCOUNT_ID"]  # pode ser com ou sem prefixo act_
META_AD_ACCOUNT = _raw_ad_account if _raw_ad_account.startswith("act_") else f"act_{_raw_ad_account}"
GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
# ──────────────────────────────────────────────────────────

SHEET_NAME = "Meta_Ads_Campanhas"
JANELA_DIAS = 500  # historico desde 01/01/2025 (modo incremental)
FALLBACK_DIAS = 500

# actions e action_values trazem arrays com todos os tipos de conversão.
# Filtramos somente o tipo "purchase" no parse abaixo.
FIELDS = (
    "campaign_id,campaign_name,date_start,date_stop,"
    "impressions,reach,clicks,spend,ctr,"
    "actions,action_values"
)

HEADERS = [
    "campaign_id", "campaign_name", "date_start", "date_stop",
    "impressions", "reach", "clicks", "spend", "ctr",
    "leads", "messaging_conversations", "contacts", "complete_registrations",
    "add_to_cart", "initiate_checkout", "purchases", "conversions_total",
    "purchase_value", "conversion_action_types",
]

CONVERSION_GROUPS = {
    "leads": [
        "lead",
        "onsite_conversion.lead_grouped",
        "offsite_conversion.fb_pixel_lead",
        "leadgen_grouped",
        "onsite_conversion.lead",
    ],
    "messaging_conversations": [
        "onsite_conversion.messaging_conversation_started_7d",
        "onsite_conversion.messaging_first_reply",
        "omni_messaging_conversation_started_7d",
    ],
    "contacts": [
        "contact",
        "contact_total",
        "offsite_conversion.fb_pixel_contact",
        "onsite_conversion.contact",
    ],
    "complete_registrations": [
        "complete_registration",
        "offsite_conversion.fb_pixel_complete_registration",
        "omni_complete_registration",
    ],
    "add_to_cart": [
        "add_to_cart",
        "offsite_conversion.fb_pixel_add_to_cart",
        "omni_add_to_cart",
    ],
    "initiate_checkout": [
        "initiate_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
        "omni_initiated_checkout",
    ],
    "purchases": [
        "purchase",
        "omni_purchase",
        "offsite_conversion.fb_pixel_purchase",
    ],
}

PURCHASE_VALUE_ACTION_TYPES = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
]


def _extrair_action(lista: list, action_type: str) -> float:
    """Extrai o valor de um action_type específico de um array de actions."""
    for item in lista:
        if item.get("action_type") == action_type:
            return float(item.get("value", 0))
    return 0.0


def _extrair_primeiro_grupo(lista: list, action_types: list) -> float:
    """Extrai a primeira variante encontrada para evitar dupla contagem."""
    for action_type in action_types:
        valor = _extrair_action(lista, action_type)
        if valor:
            return valor
    return 0.0


def _listar_action_types(lista: list) -> str:
    """Lista action_types com valor > 0 para auditoria no Sheets."""
    tipos = []
    for item in lista or []:
        try:
            valor = float(item.get("value", 0))
        except (TypeError, ValueError):
            valor = 0.0
        action_type = item.get("action_type", "")
        if action_type and valor:
            tipos.append(action_type)
    return ", ".join(sorted(set(tipos)))


def buscar_paginas(since: str, until: str) -> list:
    """Busca todos os dados paginados da API usando o link 'next' da paginacao."""
    base_query = {
        "fields": FIELDS,
        "level": "campaign",
        "time_increment": "1",
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": "500",
        "access_token": META_TOKEN,
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
            print(f"  Pagina {pagina}: {len(page_data)} registros")
            next_url = data.get("paging", {}).get("next")
            if not page_data:
                print(f"  Sem mais dados. Paging: {data.get('paging', {})}")
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
    parser.add_argument("--historico", action="store_true",
                        help="Fetch dados historicos desde 2025-01-01")
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
            print(f"  Registros neste chunk: {len(registros)}")
            current = chunk_end + timedelta(days=1)
            time.sleep(0.3)
        registros = todos_registros
        print(f"Total registros historicos obtidos: {len(registros)}")
    else:
        until = (date.today() - timedelta(days=1)).isoformat()
        since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
        print(f"Modo INCREMENTAL: Periodo: {since} → {until}")
        registros = buscar_paginas(since, until)
        print(f"Registros obtidos: {len(registros)}")

    # Converte para linhas
    rows = []
    for r in registros:
        actions = r.get("actions", [])
        action_values = r.get("action_values", [])
        conversions = {
            name: _extrair_primeiro_grupo(actions, action_types)
            for name, action_types in CONVERSION_GROUPS.items()
        }
        conversions_total = sum(conversions.values())
        purchase_value = _extrair_primeiro_grupo(action_values, PURCHASE_VALUE_ACTION_TYPES)
        conversion_action_types = _listar_action_types(actions)

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
            round(conversions["leads"], 2),
            round(conversions["messaging_conversations"], 2),
            round(conversions["contacts"], 2),
            round(conversions["complete_registrations"], 2),
            round(conversions["add_to_cart"], 2),
            round(conversions["initiate_checkout"], 2),
            round(conversions["purchases"], 2),
            round(conversions_total, 2),
            round(purchase_value, 2),
            conversion_action_types,
        ])

    # Upsert por date_start + campaign_id (evita duplicar campanhas no mesmo dia)
    upsert_por_data(SPREADSHEET_ID, SHEET_NAME, HEADERS, rows, token,
                    key_cols=["date_start", "campaign_id"])
    print("Concluido.")


if __name__ == "__main__":
    main()

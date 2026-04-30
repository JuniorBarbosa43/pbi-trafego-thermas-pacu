"""
Upload dos dados do meta_ads_campanhas.json local para o Google Sheets.
Usado para carga inicial - depois o script incremental cuida da atualizacao.

O JSON deve ser passado como arquivo no GitHub Actions via artifact ou
embutido como variavel de ambiente.

Uso:
    python scripts/upload_meta_ads_json.py --json-file data/meta_ads_campanhas.json
"""

import json
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, upsert_por_data, criar_sheet_se_nao_existe

GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID       = os.environ["SPREADSHEET_ID"]

SHEET_NAME = "Meta_Ads_Campanhas"
HEADERS    = ["campaign_id", "campaign_name", "date_start", "date_stop",
              "impressions", "reach", "clicks", "spend", "ctr",
              "leads", "messaging_conversations", "contacts", "complete_registrations",
              "add_to_cart", "initiate_checkout", "purchases", "conversions_total",
              "purchase_value", "conversion_action_types"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-file", required=True, help="Caminho para o JSON de campanhas")
    args = parser.parse_args()

    print(f"=== Upload Meta Ads JSON → Google Sheets ===")
    print(f"Lendo: {args.json_file}")

    with open(args.json_file, encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total de registros no JSON: {len(data)}")

    if not data:
        print("JSON vazio. Abortando.")
        sys.exit(1)

    rows = []
    for r in data:
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
            round(float(r.get("leads", 0)), 2),
            round(float(r.get("messaging_conversations", 0)), 2),
            round(float(r.get("contacts", 0)), 2),
            round(float(r.get("complete_registrations", 0)), 2),
            round(float(r.get("add_to_cart", 0)), 2),
            round(float(r.get("initiate_checkout", 0)), 2),
            round(float(r.get("purchases", 0)), 2),
            round(float(r.get("conversions_total", 0)), 2),
            round(float(r.get("purchase_value", 0)), 2),
            r.get("conversion_action_types", ""),
        ])

    token = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    criar_sheet_se_nao_existe(SPREADSHEET_ID, SHEET_NAME, token)

    print(f"Enviando {len(rows)} linhas para o Sheets...")
    upsert_por_data(SPREADSHEET_ID, SHEET_NAME, HEADERS, rows, token, key_cols=["campaign_id", "date_start"])
    print("Concluido!")


if __name__ == "__main__":
    main()

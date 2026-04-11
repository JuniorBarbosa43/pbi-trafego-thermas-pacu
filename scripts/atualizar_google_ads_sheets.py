→→"""
Atualiza Google Ads campanhas no Google Sheets via Google Ads API.
Roda via GitHub Actions todo dia as 06:20.
Janela de 14 dias (cobre delay de consolidacao da API).
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import urllib.error
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, limpar_e_gravar, criar_sheet_se_nao_existe

# ── Credenciais via GitHub Secrets ─────────────────────────
GOOGLE_CLIENT_ID      = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET  = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN  = os.environ["GOOGLE_REFRESH_TOKEN"]
GOOGLE_DEVELOPER_TOKEN = os.environ["GOOGLE_DEVELOPER_TOKEN"]
GOOGLE_CUSTOMER_ID    = os.environ["GOOGLE_CUSTOMER_ID"]   # conta direta: 3180978445
GOOGLE_MCC_ID         = os.environ.get("GOOGLE_MCC_ID", "6359594317")  # conta MCC gerenciadora
SPREADSHEET_ID        = os.environ["SPREADSHEET_ID"]
# ───────────────────────────────────────────────────────────

SHEET_NAME  = "Google_Ads_Campanhas"
JANELA_DIAS = 14

HEADERS = [
        "date", "campaign_id", "campaign_name", "campaign_status",
        "impressions", "clicks", "cost_micros", "cost_brl",
        "ctr", "average_cpc", "conversions", "conversions_value"
]


def obter_google_ads_token() -> str:
        """Troca refresh token por access token com scope Google Ads."""
        data = urllib.parse.urlencode({
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": GOOGLE_REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        }).encode("utf-8")

    req = urllib.request.Request(
                "https://oauth2.googleapis.com/token",
                data=data,
                method="POST"
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())["access_token"]


def buscar_google_ads(token_ads: str, since: str, until: str) -> list:
        """
            Busca dados de campanhas via Google Ads API (GAQL).
                Retorna lista de dicts com os campos.
                    """
        customer_id = GOOGLE_CUSTOMER_ID.replace("-", "")
        url = f"https://googleads.googleapis.com/v19/customers/{customer_id}/googleAds:searchStream"

    query = f"""
            SELECT
                        segments.date,
                                    campaign.id,
                                                campaign.name,
                                                            campaign.status,
                                                                        metrics.impressions,
                                                                                    metrics.clicks,
                                                                                                metrics.cost_micros,
                                                                                                            metrics.ctr,
                                                                                                                        metrics.average_cpc,
                                                                                                                                    metrics.conversions,
                                                                                                                                                metrics.conversions_value
                                                                                                                                                        FROM campaign
                                                                                                                                                                WHERE segments.date BETWEEN '{since}' AND '{until}'
                                                                                                                                                                        ORDER BY segments.date DESC
                                                                                                                                                                            """

    body = json.dumps({"query": query}).encode("utf-8")

    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {token_ads}")
    req.add_header("developer-token", GOOGLE_DEVELOPER_TOKEN)
    req.add_header("Content-Type", "application/json")
    req.add_header("login-customer-id", GOOGLE_MCC_ID.replace("-", ""))

    try:
                with urllib.request.urlopen(req) as resp:
                                raw = resp.read().decode("utf-8")
                                # searchStream retorna uma lista de objetos JSON por linha
                                results = []
            for line in raw.strip().splitlines():
                                try:
                                                        obj = json.loads(line)
                                                        results.append(obj)
except json.JSONDecodeError:
                    continue
            return results
except urllib.error.HTTPError as e:
        erro = e.read().decode("utf-8")
        print(f"AVISO Google Ads API: {e.code} - {erro[:500]}")
        if e.code in (403, 404):
                        print("Developer Token sem acesso basico aprovado. Retornando lista vazia.")
                        return []
                    raise


def extrair_rows(api_results: list) -> list:
        """Extrai linhas planas dos resultados da API."""
    rows = []
    for batch in api_results:
                for result in batch.get("results", []):
                                seg      = result.get("segments", {})
                                campaign = result.get("campaign", {})
                                metrics  = result.get("metrics", {})

            cost_micros = int(metrics.get("costMicros", 0))
            cost_brl    = round(cost_micros / 1_000_000, 2)
            ctr         = round(float(metrics.get("ctr", 0)) * 100, 4)  # em %
            avg_cpc     = round(int(metrics.get("averageCpc", 0)) / 1_000_000, 4)

            rows.append([
                                seg.get("date", ""),
                                str(campaign.get("id", "")),
                                campaign.get("name", ""),
                                campaign.get("status", ""),
                                int(metrics.get("impressions", 0)),
                                int(metrics.get("clicks", 0)),
                                cost_micros,
                                cost_brl,
                                ctr,
                                avg_cpc,
                                round(float(metrics.get("conversions", 0)), 2),
                                round(float(metrics.get("conversionsValue", 0)), 2),
            ])
    return rows


def main():
        print("=== Google Ads → Google Sheets ===")

    since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
    until = (date.today() - timedelta(days=1)).isoformat()
    print(f"Periodo: {since} → {until}")

    # Tokens
    token_ads = obter_google_ads_token()
    token_sheets = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)

    # Busca dados
    api_results = buscar_google_ads(token_ads, since, until)
    rows = extrair_rows(api_results)
    print(f"Registros obtidos: {len(rows)}")

    # Grava no Sheets
    criar_sheet_se_nao_existe(SPREADSHEET_ID, SHEET_NAME, token_sheets)
    limpar_e_gravar(SPREADSHEET_ID, SHEET_NAME, HEADERS, rows, token_sheets)

    print("Concluido.")


if __name__ == "__main__":
        main()

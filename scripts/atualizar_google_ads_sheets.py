"""
Atualiza Google Ads campanhas no Google Sheets via Google Ads API.
Roda via GitHub Actions.
Janela padrao: ultimos 14 dias.
Suporta --historico para recarga em blocos.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import List

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import criar_sheet_se_nao_existe, obter_access_token, upsert_por_data


def _pick_env(primary: str, fallback: str = "") -> str:
    value = os.environ.get(primary, "").strip()
    if value:
        return value
    if fallback:
        return os.environ.get(fallback, "").strip()
    return ""


def _only_digits(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


# Credenciais via GitHub Secrets
GOOGLE_CLIENT_ID = _pick_env("GOOGLE_ADS_CLIENT_ID", "GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = _pick_env("GOOGLE_ADS_CLIENT_SECRET", "GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = _pick_env("GOOGLE_ADS_REFRESH_TOKEN", "GOOGLE_REFRESH_TOKEN")
GOOGLE_DEVELOPER_TOKEN = _pick_env("GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_DEVELOPER_TOKEN")
GOOGLE_CUSTOMER_ID = _only_digits(_pick_env("GOOGLE_ADS_CUSTOMER_ID", "GOOGLE_CUSTOMER_ID"))
GOOGLE_MCC_ID = _only_digits(_pick_env("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "GOOGLE_MCC_ID"))
GOOGLE_ADS_API_VERSION = _pick_env("GOOGLE_ADS_API_VERSION") or "v23"
SPREADSHEET_ID = _pick_env("SPREADSHEET_ID")

SHEET_NAME = "Google_Ads_Campanhas"
JANELA_DIAS = 14

HEADERS = [
    "date",
    "campaign_id",
    "campaign_name",
    "campaign_status",
    "impressions",
    "clicks",
    "cost_micros",
    "cost_brl",
    "ctr",
    "average_cpc",
    "conversions",
    "conversions_value",
]


class GoogleAdsHttpError(RuntimeError):
    def __init__(self, version: str, status: int, url: str, body: str, content_type: str):
        super().__init__(f"Google Ads API {version} HTTP {status}")
        self.version = version
        self.status = status
        self.url = url
        self.body = body
        self.content_type = content_type

    @property
    def body_preview(self) -> str:
        return (self.body or "").strip()[:500]

    @property
    def is_html_404(self) -> bool:
        if self.status != 404:
            return False
        ctype = (self.content_type or "").lower()
        snippet = (self.body or "").lower()
        return "text/html" in ctype or "<html" in snippet or "error 404" in snippet


def validar_config() -> None:
    missing = []
    if not GOOGLE_CLIENT_ID:
        missing.append("GOOGLE_CLIENT_ID/GOOGLE_ADS_CLIENT_ID")
    if not GOOGLE_CLIENT_SECRET:
        missing.append("GOOGLE_CLIENT_SECRET/GOOGLE_ADS_CLIENT_SECRET")
    if not GOOGLE_REFRESH_TOKEN:
        missing.append("GOOGLE_REFRESH_TOKEN/GOOGLE_ADS_REFRESH_TOKEN")
    if not GOOGLE_DEVELOPER_TOKEN:
        missing.append("GOOGLE_ADS_DEVELOPER_TOKEN")
    if not GOOGLE_CUSTOMER_ID:
        missing.append("GOOGLE_ADS_CUSTOMER_ID")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")

    if missing:
        raise RuntimeError("Variaveis obrigatorias ausentes: " + ", ".join(missing))


def normalizar_versao(version: str) -> str:
    version = (version or "").strip().lower()
    if not version:
        return "v23"
    if not version.startswith("v"):
        return f"v{version}"
    return version


def versoes_tentativa() -> List[str]:
    preferida = normalizar_versao(GOOGLE_ADS_API_VERSION)
    fallback = ["v24", "v23", "v22", "v21", "v20"]
    versions = []
    for v in [preferida] + fallback:
        if v not in versions:
            versions.append(v)
    return versions


def obter_google_ads_token() -> str:
    """Troca refresh token por access token com escopo Google Ads."""
    data = urllib.parse.urlencode(
        {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": GOOGLE_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        method="POST",
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req) as resp:
        payload = json.loads(resp.read())
        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"OAuth sem access_token para Google Ads: {payload}")
        return access_token


def montar_query(since: str, until: str) -> str:
    return f"""
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


def parse_search_stream(raw: str) -> list:
    text = (raw or "").strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    # Fallback para payloads separados por linha.
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                items.append(obj)
        except json.JSONDecodeError:
            continue
    return items


def request_search_stream(version: str, token_ads: str, since: str, until: str) -> list:
    url = f"https://googleads.googleapis.com/{version}/customers/{GOOGLE_CUSTOMER_ID}/googleAds:searchStream"
    body = json.dumps({"query": montar_query(since, until)}).encode("utf-8")

    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {token_ads}")
    req.add_header("developer-token", GOOGLE_DEVELOPER_TOKEN)
    req.add_header("Content-Type", "application/json")
    if GOOGLE_MCC_ID:
        req.add_header("login-customer-id", GOOGLE_MCC_ID)

    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return parse_search_stream(raw)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        content_type = e.headers.get("Content-Type", "")
        raise GoogleAdsHttpError(version, e.code, url, body_text, content_type) from e


def buscar_google_ads(token_ads: str, since: str, until: str) -> list:
    """Busca dados via Google Ads API testando versao preferida + fallback."""
    ultimos_erros = []

    for version in versoes_tentativa():
        print(f"Tentando Google Ads API {version}...")
        try:
            return request_search_stream(version, token_ads, since, until)
        except GoogleAdsHttpError as err:
            ultimos_erros.append(err)
            print(f"AVISO Google Ads API {version}: HTTP {err.status} - {err.body_preview}")

            if err.is_html_404:
                print(
                    f"Endpoint {version} indisponivel/sunset. Tentando proxima versao..."
                )
                continue

            if err.status == 403:
                raise RuntimeError(
                    "Google Ads API retornou 403 (acesso negado). "
                    "Confira developer token, login-customer-id e vinculo da conta no MCC."
                ) from err

            if err.status == 404:
                raise RuntimeError(
                    "Google Ads API retornou 404 para a conta informada. "
                    "Confira GOOGLE_ADS_CUSTOMER_ID e GOOGLE_ADS_LOGIN_CUSTOMER_ID."
                ) from err

            raise RuntimeError(
                f"Falha na Google Ads API ({version}) HTTP {err.status}. "
                f"Detalhe: {err.body_preview}"
            ) from err

    resumo = ", ".join(
        f"{err.version}=HTTP{err.status}" for err in ultimos_erros[-3:]
    ) or "sem detalhe"
    raise RuntimeError(
        "Nao foi possivel consultar Google Ads em nenhuma versao tentada. "
        f"Resumo: {resumo}"
    )


def extrair_rows(api_results: list) -> list:
    """Extrai linhas planas dos resultados da API."""
    rows = []
    for batch in api_results:
        for result in batch.get("results", []):
            seg = result.get("segments", {})
            campaign = result.get("campaign", {})
            metrics = result.get("metrics", {})

            cost_micros = int(metrics.get("costMicros", 0))
            cost_brl = round(cost_micros / 1_000_000, 2)
            ctr = round(float(metrics.get("ctr", 0)) * 100, 4)
            avg_cpc = round(int(metrics.get("averageCpc", 0)) / 1_000_000, 4)

            rows.append(
                [
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
                ]
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Atualiza Google Ads no Google Sheets")
    parser.add_argument(
        "--historico",
        action="store_true",
        help="Busca historico desde 2025-01-01 em blocos",
    )
    args = parser.parse_args()

    validar_config()
    print("=== Google Ads -> Google Sheets ===")

    token_ads = obter_google_ads_token()
    token_sheets = obter_access_token(
        GOOGLE_CLIENT_ID,
        GOOGLE_CLIENT_SECRET,
        GOOGLE_REFRESH_TOKEN,
    )

    criar_sheet_se_nao_existe(SPREADSHEET_ID, SHEET_NAME, token_sheets)

    if args.historico:
        print("Modo HISTORICO: buscando desde 2025-01-01 em chunks de 30 dias")
        inicio = date(2025, 1, 1)
        fim = date.today()
        chunk_dias = 30
        todos_rows = []

        current = inicio
        while current < fim:
            chunk_end = min(current + timedelta(days=chunk_dias), fim)
            since = current.isoformat()
            until = chunk_end.isoformat()
            print(f"  Periodo: {since} -> {until}")

            api_results = buscar_google_ads(token_ads, since, until)
            rows = extrair_rows(api_results)
            todos_rows.extend(rows)
            print(f"    Registros neste chunk: {len(rows)}")

            current = chunk_end + timedelta(days=1)
            time.sleep(0.3)

        rows = todos_rows
        print(f"Total registros historicos obtidos: {len(rows)}")
    else:
        since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
        until = (date.today() - timedelta(days=1)).isoformat()
        print(f"Modo INCREMENTAL: Periodo: {since} -> {until}")

        api_results = buscar_google_ads(token_ads, since, until)
        rows = extrair_rows(api_results)
        print(f"Registros obtidos: {len(rows)}")

    upsert_por_data(
        SPREADSHEET_ID,
        SHEET_NAME,
        HEADERS,
        rows,
        token_sheets,
        key_cols=["date"],
    )

    print("Concluido.")


if __name__ == "__main__":
    main()

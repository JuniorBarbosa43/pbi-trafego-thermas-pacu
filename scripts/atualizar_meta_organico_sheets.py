"""
Atualiza Meta Organico (FB + IG + Posts) no Google Sheets via Graph API.
Roda via GitHub Actions todo dia as 06:10.
Modo UPSERT com opcao --historico para carga de dados passados.
"""

import json
import os
import sys
import argparse
import urllib.request
import urllib.parse
import urllib.error
from datetime import date, timedelta
import time

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, upsert_por_data, criar_sheet_se_nao_existe

META_TOKEN           = os.environ["META_TOKEN"]
META_PAGE_ID         = os.environ["META_PAGE_ID"]
META_IG_ID           = os.environ.get("META_IG_ID", "17841407112299710")
GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID       = os.environ["SPREADSHEET_ID"]

JANELA_DIAS = 90


def obter_page_token() -> str:
    url = f"https://graph.facebook.com/v25.0/me/accounts?access_token={META_TOKEN}"
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
    for page in data.get("data", []):
        if page.get("id") == META_PAGE_ID:
            return page["access_token"]
    raise ValueError(f"Pagina {META_PAGE_ID} nao encontrada")


def graph_get(path: str, params: dict) -> dict:
    url = f"https://graph.facebook.com/v25.0/{path}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        erro = e.read().decode("utf-8")
        print(f"AVISO Graph API ({path}): HTTP {e.code} - {erro[:300]}")
        return {}
    except Exception as ex:
        print(f"AVISO Graph API ({path}): {ex}")
        return {}


def atualizar_fb(token_g: str, page_token: str, historico: bool = False):
    metricas = [
        "page_video_views",
        "page_post_engagements",
        "page_views_total",
        "page_actions_post_reactions_total",
    ]

    if historico:
        print("FB: Modo HISTORICO desde 2025-01-01 em chunks de 90 dias")
        inicio = date(2025, 1, 1)
        fim = date.today()
        chunk_dias = 90
        todos_rows = []

        current = inicio
        while current < fim:
            chunk_end = min(current + timedelta(days=chunk_dias), fim)
            since = current.isoformat()
            until = chunk_end.isoformat()
            print(f"  Periodo: {since} → {until}")

            data = graph_get(f"{META_PAGE_ID}/insights", {
                "metric":       ",".join(metricas),
                "period":       "day",
                "since":        since,
                "until":        until,
                "access_token": page_token,
            })

            for item in data.get("data", []):
                metrica = item.get("name", "")
                for val in item.get("values", []):
                    v = val.get("value", 0)
                    if isinstance(v, dict):
                        v = sum(v.values())
                    todos_rows.append([val.get("end_time", "")[:10], metrica, v])

            current = chunk_end + timedelta(days=1)
            time.sleep(0.2)

        rows = todos_rows
    else:
        data = graph_get(f"{META_PAGE_ID}/insights", {
            "metric":       ",".join(metricas),
            "period":       "day",
            "since":        (date.today() - timedelta(days=JANELA_DIAS)).isoformat(),
            "until":        date.today().isoformat(),
            "access_token": page_token,
        })

        rows = []
        for item in data.get("data", []):
            metrica = item.get("name", "")
            for val in item.get("values", []):
                v = val.get("value", 0)
                if isinstance(v, dict):
                    v = sum(v.values())
                rows.append([val.get("end_time", "")[:10], metrica, v])

    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_FB", token_g)
    upsert_por_data(SPREADSHEET_ID, "Meta_Organico_FB", headers, rows, token_g, key_cols=["data", "metrica"])


def atualizar_ig(token_g: str, historico: bool = False):
    # Metricas validas na API v25+ (impressions e follower_count foram removidas)
    metricas_day = ["reach", "website_clicks", "profile_views"]
    metricas_total = ["accounts_engaged"]

    if historico:
        print("IG: Modo HISTORICO desde 2025-01-01 em chunks de 28 dias (limite da API)")
        inicio = date(2025, 1, 1)
        fim = date.today()
        chunk_dias = 28  # API IG limita a 30 dias por request
        todos_rows = []

        current = inicio
        while current < fim:
            chunk_end = min(current + timedelta(days=chunk_dias), fim)
            since = current.isoformat()
            until = chunk_end.isoformat()
            print(f"  Periodo: {since} → {until}")

            data = graph_get(f"{META_IG_ID}/insights", {
                "metric":       ",".join(metricas_day),
                "period":       "day",
                "since":        since,
                "until":        until,
                "access_token": META_TOKEN,
            })
            for item in data.get("data", []):
                metrica = item.get("name", "")
                for val in item.get("values", []):
                    todos_rows.append([val.get("end_time", "")[:10], metrica, val.get("value", 0)])

            data_total = graph_get(f"{META_IG_ID}/insights", {
                "metric":       ",".join(metricas_total),
                "period":       "total_over_range",
                "since":        since,
                "until":        until,
                "metric_type":  "total_value",
                "access_token": META_TOKEN,
            })
            for item in data_total.get("data", []):
                metrica = item.get("name", "")
                total_value = item.get("total_value", {})
                valor = total_value.get("value", 0) if isinstance(total_value, dict) else 0
                todos_rows.append([until, metrica, valor])

            current = chunk_end + timedelta(days=1)
            time.sleep(0.2)

        rows = todos_rows
    else:
        rows = []
        # API IG limita a 30 dias por request — usar 28 para margem
        janela_ig = min(JANELA_DIAS, 28)
        since = (date.today() - timedelta(days=janela_ig)).isoformat()
        until = date.today().isoformat()

        data = graph_get(f"{META_IG_ID}/insights", {
            "metric":       ",".join(metricas_day),
            "period":       "day",
            "since":        since,
            "until":        until,
            "access_token": META_TOKEN,
        })
        for item in data.get("data", []):
            metrica = item.get("name", "")
            for val in item.get("values", []):
                rows.append([val.get("end_time", "")[:10], metrica, val.get("value", 0)])

        data_total = graph_get(f"{META_IG_ID}/insights", {
            "metric":       ",".join(metricas_total),
            "period":       "total_over_range",
            "since":        since,
            "until":        until,
            "metric_type":  "total_value",
            "access_token": META_TOKEN,
        })
        for item in data_total.get("data", []):
            metrica = item.get("name", "")
            total_value = item.get("total_value", {})
            valor = total_value.get("value", 0) if isinstance(total_value, dict) else 0
            rows.append([until, metrica, valor])

    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_IG", token_g)
    upsert_por_data(SPREADSHEET_ID, "Meta_Organico_IG", headers, rows, token_g, key_cols=["data", "metrica"])


def atualizar_posts(token_g: str, historico: bool = False):
    # Nota: API do Meta retorna ~200 posts mais recentes. Para historico completo,
    # seria necessario rastrear paging. Para agora, buscamos tudo disponivel.
    data = graph_get(f"{META_IG_ID}/media", {
        "fields":       "id,timestamp,media_type,like_count,comments_count,reach,impressions",
        "limit":        "100",
        "access_token": META_TOKEN,
    })

    rows = []
    for post in data.get("data", []):
        rows.append([
            post.get("id", ""),
            post.get("timestamp", "")[:10],
            post.get("media_type", ""),
            post.get("like_count", 0),
            post.get("comments_count", 0),
            post.get("reach", 0),
            post.get("impressions", 0),
        ])

    # Colunas alinhadas com o modelo Power BI (IG_Posts.tmdl)
    headers = ["id", "data", "tipo", "likes", "comentarios", "reach", "impressions"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "IG_Posts", token_g)
    # Upsert por ID (unico por post)
    upsert_por_data(SPREADSHEET_ID, "IG_Posts", headers, rows, token_g, key_cols=["id"])


def main():
    parser = argparse.ArgumentParser(description="Atualiza Meta Organico no Google Sheets")
    parser.add_argument("--historico", action="store_true", help="Fetch dados historicos desde 2025-01-01")
    args = parser.parse_args()

    print("=== Meta Organico → Google Sheets ===")
    page_token = obter_page_token()
    token_g = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)

    print("FB Insights...")
    atualizar_fb(token_g, page_token, historico=args.historico)

    print("IG Insights...")
    atualizar_ig(token_g, historico=args.historico)

    print("IG Posts...")
    atualizar_posts(token_g, historico=args.historico)

    print("Concluido.")


if __name__ == "__main__":
    main()

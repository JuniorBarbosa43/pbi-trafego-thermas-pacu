"""
Atualiza Meta Organico (FB + IG + Posts) no Google Sheets via Graph API.
Roda via GitHub Actions todo dia as 06:10.
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

META_TOKEN           = os.environ["META_TOKEN"]
META_PAGE_ID         = os.environ["META_PAGE_ID"]
META_IG_ID           = os.environ["META_IG_ID"]
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


def atualizar_fb(token_g: str, page_token: str):
    metricas = [
        "page_video_views",
        "page_post_engagements",
        "page_views_total",
        "page_actions_post_reactions_total",
    ]

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

    headers = ["date", "metric", "value"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_FB", token_g)
    limpar_e_gravar(SPREADSHEET_ID, "Meta_Organico_FB", headers, rows, token_g)


def atualizar_ig(token_g: str):
    metricas_day = ["reach", "impressions", "follower_count", "website_clicks"]
    metricas_total = ["accounts_engaged", "profile_views"]

    rows = []
    since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
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

    headers = ["date", "metric", "value"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_IG", token_g)
    limpar_e_gravar(SPREADSHEET_ID, "Meta_Organico_IG", headers, rows, token_g)


def atualizar_posts(token_g: str):
    data = graph_get(f"{META_IG_ID}/media", {
        "fields":       "id,timestamp,media_type,like_count,comments_count,reach,impressions",
        "limit":        "50",
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

    headers = ["id", "date", "media_type", "likes", "comments", "reach", "impressions"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "IG_Posts", token_g)
    limpar_e_gravar(SPREADSHEET_ID, "IG_Posts", headers, rows, token_g)


def main():
    print("=== Meta Organico → Google Sheets ===")
    page_token = obter_page_token()
    token_g = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)

    print("FB Insights...")
    atualizar_fb(token_g, page_token)

    print("IG Insights...")
    atualizar_ig(token_g)

    print("IG Posts...")
    atualizar_posts(token_g)

    print("Concluido.")


if __name__ == "__main__":
    main()

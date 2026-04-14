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
    # Metricas principais (comprovadas na API v25)
    metricas_core = [
        "page_video_views",
        "page_post_engagements",
        "page_views_total",
        "page_actions_post_reactions_total",
    ]
    # Metricas extras -- buscadas em chunks de 90 dias (limite da API)
    metricas_extras = [
        "page_impressions_unique",
        "page_daily_follows_unique",
        "page_daily_unfollows_unique",
    ]
    metricas = metricas_core

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
            print(f"  Periodo: {since} -> {until}")

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
        since = (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
        until = date.today().isoformat()
        data = graph_get(f"{META_PAGE_ID}/insights", {
            "metric":       ",".join(metricas),
            "period":       "day",
            "since":        since,
            "until":        until,
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

    # Metricas extras em chunks de 90 dias (limite da API Facebook)
    if historico:
        extra_inicio = date(2025, 1, 1)
    else:
        extra_inicio = date.today() - timedelta(days=JANELA_DIAS)
    extra_fim = date.today()
    CHUNK_EXTRA = 90

    for extra_metric in metricas_extras:
        print(f"  FB extra: {extra_metric}...")
        extra_current = extra_inicio
        while extra_current <= extra_fim:
            extra_chunk_end = min(extra_current + timedelta(days=CHUNK_EXTRA), extra_fim)
            extra_data = graph_get(f"{META_PAGE_ID}/insights", {
                "metric":       extra_metric,
                "period":       "day",
                "since":        extra_current.isoformat(),
                "until":        extra_chunk_end.isoformat(),
                "access_token": page_token,
            })
            if extra_data.get("data"):
                for item in extra_data["data"]:
                    metrica = item.get("name", "")
                    for val in item.get("values", []):
                        v = val.get("value", 0)
                        if isinstance(v, dict):
                            v = sum(v.values())
                        rows.append([val.get("end_time", "")[:10], metrica, v])
                print(f"    chunk {extra_current} -> {extra_chunk_end}: {len(extra_data['data'])} series")
            else:
                print(f"    chunk {extra_current} -> {extra_chunk_end}: SEM DADOS")
            extra_current = extra_chunk_end + timedelta(days=1)
            time.sleep(0.2)

    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_FB", token_g)
    upsert_por_data(SPREADSHEET_ID, "Meta_Organico_FB", headers, rows, token_g, key_cols=["data", "metrica"])


def atualizar_ig(token_g: str, page_token: str, historico: bool = False):
    metricas_day = ["reach"]
    metricas_total_value = ["website_clicks", "profile_views", "accounts_engaged"]
    ig_token = page_token or META_TOKEN

    def _buscar_chunk_ig(since, until):
        chunk_rows = []
        if metricas_day:
            data = graph_get(f"{META_IG_ID}/insights", {
                "metric":       ",".join(metricas_day),
                "period":       "day",
                "since":        since,
                "until":        until,
                "access_token": ig_token,
            })
            if not data.get("data"):
                print(f"  AVISO IG insights (day): sem dados para {since}->{until}")
            for item in data.get("data", []):
                metrica = item.get("name", "")
                for val in item.get("values", []):
                    chunk_rows.append([val.get("end_time", "")[:10], metrica, val.get("value", 0)])
        for tv_metric in metricas_total_value:
            tv_data = graph_get(f"{META_IG_ID}/insights", {
                "metric":       tv_metric,
                "period":       "day",
                "metric_type":  "total_value",
                "since":        since,
                "until":        until,
                "access_token": ig_token,
            })
            if tv_data.get("data"):
                for item in tv_data["data"]:
                    metrica = item.get("name", "")
                    total_value = item.get("total_value", {})
                    valor = total_value.get("value", 0) if isinstance(total_value, dict) else 0
                    chunk_rows.append([until, metrica, valor])
        return chunk_rows

    if historico:
        print("IG: Modo HISTORICO desde 2025-01-01 em chunks de 28 dias")
        inicio = date(2025, 1, 1)
        fim = date.today()
        chunk_dias = 28
        todos_rows = []
        current = inicio
        while current < fim:
            chunk_end = min(current + timedelta(days=chunk_dias), fim)
            since = current.isoformat()
            until = chunk_end.isoformat()
            print(f"  Periodo: {since} -> {until}")
            todos_rows.extend(_buscar_chunk_ig(since, until))
            current = chunk_end + timedelta(days=1)
            time.sleep(0.2)
        rows = todos_rows
    else:
        janela_ig = min(JANELA_DIAS, 28)
        since = (date.today() - timedelta(days=janela_ig)).isoformat()
        until = date.today().isoformat()
        rows = _buscar_chunk_ig(since, until)

    print("  Buscando follower_count via IG user fields...")
    ig_user = graph_get(META_IG_ID, {
        "fields":       "followers_count",
        "access_token": ig_token,
    })
    fc = ig_user.get("followers_count")
    if fc is not None:
        rows.append([date.today().isoformat(), "follower_count", fc])
        print(f"  follower_count = {fc}")
    else:
        print("  AVISO: follower_count nao retornado")
    print(f"  IG total linhas coletadas: {len(rows)}")
    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_IG", token_g)
    upsert_por_data(SPREADSHEET_ID, "Meta_Organico_IG", headers, rows, token_g, key_cols=["data", "metrica"])


def atualizar_posts(token_g, page_token, historico=False):
    ig_token = page_token or META_TOKEN
    all_posts = []
    data = graph_get(f"{META_IG_ID}/media", {
        "fields":       "id,timestamp,media_type,like_count,comments_count,reach,impressions,saved",
        "limit":        "100",
        "access_token": ig_token,
    })
    all_posts.extend(data.get("data", []))
    if historico:
        next_url = data.get("paging", {}).get("next")
        page_num = 1
        while next_url and page_num < 20:
            print(f"  IG Posts paginacao {page_num + 1}...")
            try:
                with urllib.request.urlopen(next_url) as resp:
                    data = json.loads(resp.read())
                all_posts.extend(data.get("data", []))
                next_url = data.get("paging", {}).get("next")
                page_num += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"  AVISO paginacao: {e}")
                break
    rows = []
    for post in all_posts:
        rows.append([
            post.get("id", ""),
            post.get("timestamp", "")[:10],
            post.get("media_type", ""),
            post.get("like_count", 0),
            post.get("comments_count", 0),
            post.get("reach", 0),
            post.get("impressions", 0),
            post.get("saved", 0),
        ])
    print(f"  IG Posts coletados: {len(rows)}")
    headers = ["id", "data", "tipo", "likes", "comentarios", "reach", "impressions", "saved"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "IG_Posts", token_g)
    upsert_por_data(SPREADSHEET_ID, "IG_Posts", headers, rows, token_g, key_cols=["id"])


def main():
    parser = argparse.ArgumentParser(description="Atualiza Meta Organico no Google Sheets")
    parser.add_argument("--historico", action="store_true", help="Fetch dados historicos desde 2025-01-01")
    args = parser.parse_args()
    print("=== Meta Organico -> Google Sheets ===")
    page_token = obter_page_token()
    token_g = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    print("FB Insights...")
    atualizar_fb(token_g, page_token, historico=args.historico)
    print("IG Insights...")
    atualizar_ig(token_g, page_token, historico=args.historico)
    print("IG Posts...")
    atualizar_posts(token_g, page_token, historico=args.historico)
    print("Concluido.")


if __name__ == "__main__":
    main()

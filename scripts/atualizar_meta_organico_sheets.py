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
                    "page_follows",
                    "page_unfollows",
                    "page_impressions_unique",
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


def atualizar_ig(token_g: str, page_token: str, historico: bool = False):
        """
            Atualiza metricas organicas do Instagram Business.
                Usa page_token (nao user token) para garantir permissao instagram_manage_insights.
                    Tambem busca follower_count via endpoint /fields do IG user.
                        """
    metricas_day = ["reach", "website_clicks", "profile_views"]
    metricas_total = ["accounts_engaged"]

    # Tenta page_token primeiro, fallback para META_TOKEN
    ig_token = page_token or META_TOKEN

    def _buscar_chunk_ig(since: str, until: str) -> list:
                """Busca metricas IG para um periodo e retorna linhas."""
        chunk_rows = []

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

        data_total = graph_get(f"{META_IG_ID}/insights", {
                        "metric":       ",".join(metricas_total),
                        "period":       "total_over_range",
                        "since":        since,
                        "until":        until,
                        "metric_type":  "total_value",
                        "access_token": ig_token,
        })
        if not data_total.get("data"):
                        print(f"  AVISO IG insights (total): sem dados para {since}->{until}")
        for item in data_total.get("data", []):
                        metrica = item.get("name", "")
            total_value = item.get("total_value", {})
            valor = total_value.get("value", 0) if isinstance(total_value, dict) else 0
            chunk_rows.append([until, metrica, valor])

        return chunk_rows

    if historico:
                print("IG: Modo HISTORICO desde 2025-01-01 em chunks de 28 dias (limite da API)")
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

    # Buscar follower_count via endpoint de fields (disponivel fora de /insights)
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
        print("  AVISO: follower_count nao retornado (verificar permissoes do token)")

    print(f"  IG total linhas coletadas: {len(rows)}")
    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_IG", token_g)
    upsert_por_data(SPREADSHEET_ID, "Meta_Organico_IG", headers, rows, token_g, key_cols=["data", "metrica"])


def atualizar_posts(token_g: str, page_token: str, historico: bool = False):
        """
            Busca posts recentes do Instagram com metricas de engajamento.
                Usa page_token para acessar reach/impressions/saved (requer instagram_manage_insights).
                    Suporta paginacao para buscar mais de 100 posts.
                        """
    ig_token = page_token or META_TOKEN
    all_posts = []

    # Primeira pagina
    data = graph_get(f"{META_IG_ID}/media", {
                "fields":       "id,timestamp,media_type,like_count,comments_count,reach,impressions,saved",
                "limit":        "100",
                "access_token": ig_token,
    })
    all_posts.extend(data.get("data", []))

    # Paginacao (se historico, busca todas as paginas disponiveis)
    if historico:
                next_url = data.get("paging", {}).get("next")
        page_num = 1
        while next_url and page_num < 20:  # limite de seguranca
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
    # Colunas alinhadas com o modelo Power BI (IG_Posts)
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

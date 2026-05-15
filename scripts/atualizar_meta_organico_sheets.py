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
from sheets_helper import limpar_e_gravar, obter_access_token, upsert_por_data, criar_sheet_se_nao_existe

META_TOKEN           = os.environ["META_TOKEN"]
META_PAGE_ID         = os.environ["META_PAGE_ID"]
META_IG_ID           = os.environ.get("META_IG_ID", "17841407112299710")
GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID       = os.environ["SPREADSHEET_ID"]

JANELA_DIAS = 90
DEFAULT_HISTORICO_START_DATE = "2024-01-01"
MAX_POST_PAGES = 50
REQUEST_SLEEP = 0.2
API_TIMEOUT = 60
IG_MEDIA_EXTRA_METRICS_ATIVAS = True
IG_STORY_METRICS_ATIVAS = True
FB_STORY_INSIGHTS_ATIVOS = True


def obter_page_token() -> str:
    url = f"https://graph.facebook.com/v25.0/me/accounts?access_token={META_TOKEN}"
    with urllib.request.urlopen(url, timeout=API_TIMEOUT) as resp:
        data = json.loads(resp.read())
    for page in data.get("data", []):
        if page.get("id") == META_PAGE_ID:
            return page["access_token"]
    raise ValueError(f"Pagina {META_PAGE_ID} nao encontrada")


def graph_get(path: str, params: dict) -> dict:
    url = f"https://graph.facebook.com/v25.0/{path}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=API_TIMEOUT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        erro = e.read().decode("utf-8")
        print(f"AVISO Graph API ({path}): HTTP {e.code} - {erro[:300]}")
        return {}
    except Exception as ex:
        print(f"AVISO Graph API ({path}): {ex}")
        return {}


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Data invalida: {value}. Use YYYY-MM-DD.") from exc


def normalizar_valor(value):
    if isinstance(value, dict):
        return sum(v for v in value.values() if isinstance(v, (int, float)))
    return value or 0


def rows_from_insights(data: dict) -> list:
    rows = []
    for item in data.get("data", []):
        metrica = item.get("name", "")
        for val in item.get("values", []):
            rows.append([val.get("end_time", "")[:10], metrica, normalizar_valor(val.get("value", 0))])
    return rows


def extrair_summary_count(obj: dict, key: str) -> int:
    node = obj.get(key, {})
    if not isinstance(node, dict):
        return 0
    summary = node.get("summary", {})
    if not isinstance(summary, dict):
        return 0
    return int(summary.get("total_count", 0) or 0)


def inteiro(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def taxa(numerador: int, denominador: int) -> float:
    if not denominador:
        return 0
    return round(numerador / denominador, 6)


def buscar_campos_numericos(path: str, fields: list, token: str) -> dict:
    valores = {}
    for field in fields:
        data = graph_get(path, {
            "fields":       field,
            "access_token": token,
        })
        if field in data:
            valores[field] = inteiro(data.get(field))
        time.sleep(REQUEST_SLEEP)
    return valores


def gravar_dados(sheet_name: str, headers: list, rows: list, token_g: str, key_cols: list, historico: bool):
    if historico:
        limpar_e_gravar(SPREADSHEET_ID, sheet_name, headers, rows, token_g)
        return
    upsert_por_data(SPREADSHEET_ID, sheet_name, headers, rows, token_g, key_cols=key_cols)


def atualizar_fb(token_g: str, page_token: str, historico: bool = False, start_date: date = None):
    start_date = start_date or parse_date(DEFAULT_HISTORICO_START_DATE)
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
        print(f"FB: Modo HISTORICO desde {start_date.isoformat()} em chunks de 90 dias")
        inicio = start_date
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

            todos_rows.extend(rows_from_insights(data))

            current = chunk_end + timedelta(days=1)
            time.sleep(REQUEST_SLEEP)

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
        rows = rows_from_insights(data)

    # Metricas extras em chunks de 90 dias (limite da API Facebook)
    if historico:
        extra_inicio = start_date
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
                rows.extend(rows_from_insights(extra_data))
                print(f"    chunk {extra_current} -> {extra_chunk_end}: {len(extra_data['data'])} series")
            else:
                print(f"    chunk {extra_current} -> {extra_chunk_end}: SEM DADOS")
            extra_current = extra_chunk_end + timedelta(days=1)
            time.sleep(REQUEST_SLEEP)

    print("  FB campos atuais da pagina...")
    page_fields = buscar_campos_numericos(META_PAGE_ID, [
        "followers_count",
        "fan_count",
        "talking_about_count",
        "were_here_count",
        "checkins",
    ], page_token)
    hoje = date.today().isoformat()
    for field, value in page_fields.items():
        rows.append([hoje, f"page_{field}", value])
        print(f"    page_{field} = {value}")

    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_FB", token_g)
    gravar_dados("Meta_Organico_FB", headers, rows, token_g, key_cols=["data", "metrica"], historico=historico)


def atualizar_ig(token_g: str, page_token: str, historico: bool = False, start_date: date = None):
    start_date = start_date or parse_date(DEFAULT_HISTORICO_START_DATE)
    metricas_day = ["reach"]
    metricas_total_value = [
        "accounts_engaged",
        "total_interactions",
        "views",
        "profile_links_taps",
        # Mantidas para compatibilidade com contas/versoes que ainda retornam.
        "website_clicks",
        "profile_views",
    ]
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
        print(f"IG: Modo HISTORICO desde {start_date.isoformat()} em chunks de 28 dias")
        inicio = start_date
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
            time.sleep(REQUEST_SLEEP)
        rows = todos_rows
    else:
        janela_ig = min(JANELA_DIAS, 28)
        since = (date.today() - timedelta(days=janela_ig)).isoformat()
        until = date.today().isoformat()
        rows = _buscar_chunk_ig(since, until)

    print("  Buscando campos atuais do IG user...")
    ig_fields = buscar_campos_numericos(META_IG_ID, [
        "followers_count",
        "media_count",
        "follows_count",
    ], ig_token)
    hoje = date.today().isoformat()
    for field, value in ig_fields.items():
        rows.append([hoje, field, value])
        print(f"  {field} = {value}")
    print(f"  IG total linhas coletadas: {len(rows)}")
    headers = ["data", "metrica", "valor"]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "Meta_Organico_IG", token_g)
    gravar_dados("Meta_Organico_IG", headers, rows, token_g, key_cols=["data", "metrica"], historico=historico)


def obter_ig_media_insights(media_id: str, ig_token: str) -> dict:
    global IG_MEDIA_EXTRA_METRICS_ATIVAS
    metricas_base = ["reach", "saved", "views", "total_interactions", "shares"]
    metricas_extra = ["impressions", "follows", "profile_activity", "profile_visits"]

    insights = {}
    data = graph_get(f"{media_id}/insights", {
        "metric":       ",".join(metricas_base),
        "access_token": ig_token,
    })
    for item in data.get("data", []):
        nome = item.get("name", "")
        valores = item.get("values", [])
        if valores:
            insights[nome] = normalizar_valor(valores[0].get("value", 0))

    if IG_MEDIA_EXTRA_METRICS_ATIVAS:
        extra_data = graph_get(f"{media_id}/insights", {
            "metric":       ",".join(metricas_extra),
            "access_token": ig_token,
        })
        if extra_data.get("data"):
            for item in extra_data["data"]:
                nome = item.get("name", "")
                valores = item.get("values", [])
                if valores:
                    insights[nome] = normalizar_valor(valores[0].get("value", 0))
        else:
            IG_MEDIA_EXTRA_METRICS_ATIVAS = False
            print("  AVISO: metricas extras de IG posts nao retornaram dados; seguindo apenas com metricas base.")
    return insights


def obter_story_insights(media_id: str, token: str, metricas: list) -> dict:
    data = graph_get(f"{media_id}/insights", {
        "metric":       ",".join(metricas),
        "access_token": token,
    })
    insights = {}
    for item in data.get("data", []):
        nome = item.get("name", "")
        valores = item.get("values", [])
        if valores:
            valor = valores[0].get("value", 0)
            if nome == "navigation" and isinstance(valor, dict):
                insights["navigation_total"] = normalizar_valor(valor)
                for key, item_value in valor.items():
                    insights[f"navigation_{key}"] = normalizar_valor(item_value)
            else:
                insights[nome] = normalizar_valor(valor)
    return insights


def atualizar_ig_stories(token_g, page_token):
    ig_token = page_token or META_TOKEN
    rows = []
    data = graph_get(f"{META_IG_ID}/stories", {
        "fields":       "id,timestamp,media_type,media_url,permalink,thumbnail_url",
        "limit":        "100",
        "access_token": ig_token,
    })
    if not data.get("data"):
        data = graph_get(f"{META_IG_ID}/stories", {
            "fields":       "id,timestamp,media_type,media_url",
            "limit":        "100",
            "access_token": ig_token,
        })

    stories = data.get("data", [])
    print(f"  IG Stories ativos: {len(stories)}")
    metricas_story = ["reach", "replies", "navigation"]

    for index, story in enumerate(stories, start=1):
        if index == 1 or index % 10 == 0:
            print(f"  IG Stories insights: {index}/{len(stories)}")
        insights = {}
        if IG_STORY_METRICS_ATIVAS:
            insights = obter_story_insights(story.get("id", ""), ig_token, metricas_story)
            if not insights:
                print("  AVISO: insights deste IG Story nao retornaram dados; seguindo com metadados.")
        reach = inteiro(insights.get("reach", 0))
        replies = inteiro(insights.get("replies", 0))
        navigation_total = inteiro(insights.get("navigation_total", 0))
        taps_forward = inteiro(
            insights.get("navigation_tap_forward", insights.get("navigation_taps_forward", 0))
        )
        taps_back = inteiro(
            insights.get("navigation_tap_back", insights.get("navigation_taps_back", 0))
        )
        exits = inteiro(insights.get("navigation_exit", insights.get("navigation_exits", 0)))
        swipe_forward = inteiro(insights.get("navigation_swipe_forward", 0))
        rows.append([
            story.get("id", ""),
            story.get("timestamp", "")[:10],
            story.get("timestamp", ""),
            story.get("media_type", ""),
            story.get("media_url", ""),
            story.get("thumbnail_url", ""),
            story.get("permalink", ""),
            reach,
            replies,
            taps_forward,
            taps_back,
            exits,
            swipe_forward,
            navigation_total,
            taxa(replies, reach),
            taxa(exits, reach),
        ])
        time.sleep(REQUEST_SLEEP)

    headers = [
        "id", "data", "timestamp", "tipo", "media_url", "thumbnail_url", "permalink",
        "reach", "replies", "taps_forward", "taps_back", "exits",
        "swipe_forward", "navigation_total", "reply_rate_reach", "exit_rate_reach"
    ]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "IG_Stories", token_g)
    upsert_por_data(SPREADSHEET_ID, "IG_Stories", headers, rows, token_g, key_cols=["id"])


def atualizar_posts(token_g, page_token, historico=False, start_date: date = None):
    start_date = start_date or parse_date(DEFAULT_HISTORICO_START_DATE)
    ig_token = page_token or META_TOKEN
    all_posts = []
    stop_pagination = False
    campos_ig_post = "id,timestamp,caption,permalink,media_url,thumbnail_url,media_type,media_product_type,like_count,comments_count,is_comment_enabled"
    data = graph_get(f"{META_IG_ID}/media", {
        "fields":       campos_ig_post,
        "limit":        "100",
        "access_token": ig_token,
    })
    if not data.get("data"):
        campos_ig_post = "id,timestamp,caption,permalink,media_url,thumbnail_url,media_type,media_product_type,like_count,comments_count"
        data = graph_get(f"{META_IG_ID}/media", {
            "fields":       campos_ig_post,
            "limit":        "100",
            "access_token": ig_token,
        })
    for post in data.get("data", []):
        post_date = parse_date(post.get("timestamp", "")[:10]) if post.get("timestamp") else date.today()
        if historico and post_date < start_date:
            stop_pagination = True
            continue
        all_posts.append(post)
    if historico:
        next_url = data.get("paging", {}).get("next")
        page_num = 1
        while next_url and page_num < MAX_POST_PAGES and not stop_pagination:
            print(f"  IG Posts paginacao {page_num + 1}...")
            try:
                with urllib.request.urlopen(next_url, timeout=API_TIMEOUT) as resp:
                    data = json.loads(resp.read())
                for post in data.get("data", []):
                    post_date = parse_date(post.get("timestamp", "")[:10]) if post.get("timestamp") else date.today()
                    if post_date < start_date:
                        stop_pagination = True
                        continue
                    all_posts.append(post)
                next_url = data.get("paging", {}).get("next")
                page_num += 1
                time.sleep(REQUEST_SLEEP)
            except Exception as e:
                print(f"  AVISO paginacao: {e}")
                break
    rows = []
    for index, post in enumerate(all_posts, start=1):
        if index == 1 or index % 25 == 0:
            print(f"  IG Posts insights: {index}/{len(all_posts)}")
        insights = obter_ig_media_insights(post.get("id", ""), ig_token)
        time.sleep(REQUEST_SLEEP)
        likes = inteiro(post.get("like_count", 0))
        comentarios = inteiro(post.get("comments_count", 0))
        saved = inteiro(insights.get("saved", 0))
        shares = inteiro(insights.get("shares", 0))
        total_interactions = inteiro(insights.get("total_interactions", 0))
        reach = inteiro(insights.get("reach", 0))
        engagement_total = total_interactions or (likes + comentarios + saved + shares)
        rows.append([
            post.get("id", ""),
            post.get("timestamp", "")[:10],
            post.get("media_type", ""),
            post.get("media_product_type", ""),
            post.get("caption", ""),
            post.get("permalink", ""),
            post.get("media_url", ""),
            post.get("thumbnail_url", ""),
            likes,
            comentarios,
            str(post.get("is_comment_enabled", "")),
            reach,
            inteiro(insights.get("impressions", 0)),
            saved,
            inteiro(insights.get("views", 0)),
            shares,
            total_interactions,
            inteiro(insights.get("follows", 0)),
            inteiro(insights.get("profile_activity", 0)),
            inteiro(insights.get("profile_visits", 0)),
            engagement_total,
            taxa(engagement_total, reach),
        ])
    print(f"  IG Posts coletados: {len(rows)}")
    headers = [
        "id", "data", "tipo", "produto", "caption", "permalink", "media_url", "thumbnail_url",
        "likes", "comentarios", "comentarios_habilitados", "reach", "impressions", "saved", "views",
        "shares", "total_interactions", "follows", "profile_activity", "profile_visits",
        "engagement_total", "engagement_rate_reach"
    ]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "IG_Posts", token_g)
    gravar_dados("IG_Posts", headers, rows, token_g, key_cols=["id"], historico=historico)


def atualizar_fb_posts(token_g, page_token, historico=False, start_date: date = None):
    start_date = start_date or parse_date(DEFAULT_HISTORICO_START_DATE)
    since = start_date.isoformat() if historico else (date.today() - timedelta(days=JANELA_DIAS)).isoformat()
    until = date.today().isoformat()
    reaction_fields = [
        "reactions.type(LIKE).summary(total_count).limit(0).as(reactions_like)",
        "reactions.type(LOVE).summary(total_count).limit(0).as(reactions_love)",
        "reactions.type(HAHA).summary(total_count).limit(0).as(reactions_haha)",
        "reactions.type(WOW).summary(total_count).limit(0).as(reactions_wow)",
        "reactions.type(SAD).summary(total_count).limit(0).as(reactions_sad)",
        "reactions.type(ANGRY).summary(total_count).limit(0).as(reactions_angry)",
    ]
    fields = ",".join([
        "id",
        "created_time",
        "message",
        "permalink_url",
        "full_picture",
        "status_type",
        "shares",
        "comments.summary(true).limit(0)",
        "reactions.summary(total_count).limit(0)",
    ] + reaction_fields)
    rows = []
    data = graph_get(f"{META_PAGE_ID}/posts", {
        "fields":       fields,
        "limit":        "100",
        "since":        since,
        "until":        until,
        "access_token": page_token,
    })
    if not data.get("data"):
        fields = ",".join([
            "id",
            "created_time",
            "message",
            "permalink_url",
            "full_picture",
            "status_type",
            "shares",
            "comments.summary(true).limit(0)",
            "reactions.summary(total_count).limit(0)",
        ])
        data = graph_get(f"{META_PAGE_ID}/posts", {
            "fields":       fields,
            "limit":        "100",
            "since":        since,
            "until":        until,
            "access_token": page_token,
        })
    page_num = 1
    while True:
        for post in data.get("data", []):
            shares = inteiro((post.get("shares") or {}).get("count", 0))
            comentarios = extrair_summary_count(post, "comments")
            reacoes = extrair_summary_count(post, "reactions")
            reacoes_like = extrair_summary_count(post, "reactions_like")
            reacoes_love = extrair_summary_count(post, "reactions_love")
            reacoes_haha = extrair_summary_count(post, "reactions_haha")
            reacoes_wow = extrair_summary_count(post, "reactions_wow")
            reacoes_sad = extrair_summary_count(post, "reactions_sad")
            reacoes_angry = extrair_summary_count(post, "reactions_angry")
            rows.append([
                post.get("id", ""),
                post.get("created_time", "")[:10],
                post.get("message", ""),
                post.get("permalink_url", ""),
                post.get("full_picture", ""),
                post.get("status_type", ""),
                shares,
                comentarios,
                reacoes,
                reacoes_like,
                reacoes_love,
                reacoes_haha,
                reacoes_wow,
                reacoes_sad,
                reacoes_angry,
                shares + comentarios + reacoes,
            ])
        next_url = data.get("paging", {}).get("next")
        if not historico or not next_url or page_num >= MAX_POST_PAGES:
            break
        print(f"  FB Posts paginacao {page_num + 1}...")
        try:
            with urllib.request.urlopen(next_url, timeout=API_TIMEOUT) as resp:
                data = json.loads(resp.read())
            page_num += 1
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"  AVISO FB posts paginacao: {e}")
            break

    print(f"  FB Posts coletados: {len(rows)}")
    headers = [
        "id", "data", "message", "permalink_url", "full_picture", "status_type",
        "shares", "comentarios", "reacoes", "reacoes_like", "reacoes_love",
        "reacoes_haha", "reacoes_wow", "reacoes_sad", "reacoes_angry", "engagement_total"
    ]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "FB_Posts", token_g)
    gravar_dados("FB_Posts", headers, rows, token_g, key_cols=["id"], historico=historico)


def obter_fb_story_insights(story_id: str, page_token: str) -> dict:
    if not FB_STORY_INSIGHTS_ATIVOS:
        return {}

    insights = {}
    for metricas in [
        ["post_impressions", "post_impressions_unique", "post_engaged_users"],
        ["post_video_views"],
    ]:
        data = graph_get(f"{story_id}/insights", {
            "metric":       ",".join(metricas),
            "access_token": page_token,
        })
        if data.get("data"):
            for item in data["data"]:
                nome = item.get("name", "")
                valores = item.get("values", [])
                if valores:
                    insights[nome] = normalizar_valor(valores[0].get("value", 0))
        else:
            print("  AVISO: alguns insights deste FB Story nao retornaram dados; seguindo com metadados.")
    return insights


def buscar_fb_stories(page_token: str) -> list:
    full_fields = ",".join([
        "id",
        "created_time",
        "message",
        "permalink_url",
        "full_picture",
        "status_type",
    ])
    tentativas = [
        ("page_token page_id/stories", f"{META_PAGE_ID}/stories", full_fields, page_token),
        ("page_token page_id/stories_min", f"{META_PAGE_ID}/stories", "id,created_time", page_token),
        ("page_token me/stories", "me/stories", full_fields, page_token),
        ("page_token me/stories_min", "me/stories", "id,created_time", page_token),
    ]

    for label, path, fields, token in tentativas:
        data = graph_get(path, {
            "fields":       fields,
            "limit":        "100",
            "access_token": token,
        })
        stories = data.get("data", [])
        print(f"  FB Stories tentativa {label}: {len(stories)} itens")
        if stories:
            return stories
    return []


def buscar_ig_stories_para_fb_fallback(ig_token: str) -> list:
    data = graph_get(f"{META_IG_ID}/stories", {
        "fields":       "id,timestamp,media_type,media_url,permalink,thumbnail_url",
        "limit":        "100",
        "access_token": ig_token,
    })
    return data.get("data", [])


def atualizar_fb_stories(token_g, page_token):
    stories = buscar_fb_stories(page_token)
    usando_ig_fallback = False
    if not stories:
        stories = buscar_ig_stories_para_fb_fallback(page_token or META_TOKEN)
        usando_ig_fallback = bool(stories)
        if usando_ig_fallback:
            print("  FB Stories: API da Page retornou 0; usando fallback de IG Stories crosspostados.")
    print(f"  FB Stories ativos: {len(stories)}")

    rows = []
    for index, story in enumerate(stories, start=1):
        if index == 1 or index % 10 == 0:
            print(f"  FB Stories insights: {index}/{len(stories)}")
        if usando_ig_fallback:
            insights = obter_story_insights(story.get("id", ""), page_token or META_TOKEN, ["reach", "replies", "navigation"])
            reach = inteiro(insights.get("reach", 0))
            impressions = 0
            engaged_users = inteiro(insights.get("replies", 0))
            video_views = 0
            row_id = f"ig_crosspost:{story.get('id', '')}"
            created_time = story.get("timestamp", "")
            permalink_url = story.get("permalink", "")
            full_picture = story.get("media_url", story.get("thumbnail_url", ""))
            status_type = "IG_CROSSPOST_FALLBACK"
        else:
            insights = obter_fb_story_insights(story.get("id", ""), page_token)
            impressions = inteiro(insights.get("post_impressions", 0))
            reach = inteiro(insights.get("post_impressions_unique", 0))
            engaged_users = inteiro(insights.get("post_engaged_users", 0))
            video_views = inteiro(insights.get("post_video_views", 0))
            row_id = story.get("id", "")
            created_time = story.get("created_time", "")
            permalink_url = story.get("permalink_url", "")
            full_picture = story.get("full_picture", "")
            status_type = story.get("status_type", "")
        rows.append([
            row_id,
            created_time[:10],
            created_time,
            story.get("message", ""),
            permalink_url,
            full_picture,
            status_type,
            reach,
            impressions,
            engaged_users,
            video_views,
            taxa(engaged_users, reach),
        ])
        time.sleep(REQUEST_SLEEP)

    headers = [
        "id", "data", "created_time", "message", "permalink_url", "full_picture", "status_type",
        "reach", "impressions", "engaged_users", "video_views", "engagement_rate_reach"
    ]
    criar_sheet_se_nao_existe(SPREADSHEET_ID, "FB_Stories", token_g)
    upsert_por_data(SPREADSHEET_ID, "FB_Stories", headers, rows, token_g, key_cols=["id"])


def main():
    parser = argparse.ArgumentParser(description="Atualiza Meta Organico no Google Sheets")
    parser.add_argument("--historico", action="store_true", help="Fetch dados historicos desde a data inicial")
    parser.add_argument("--start-date", default=DEFAULT_HISTORICO_START_DATE, help="Data inicial do historico (YYYY-MM-DD)")
    args = parser.parse_args()
    start_date = parse_date(args.start_date)
    print("=== Meta Organico -> Google Sheets ===")
    page_token = obter_page_token()
    token_g = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    print("FB Insights...")
    atualizar_fb(token_g, page_token, historico=args.historico, start_date=start_date)
    print("FB Posts...")
    atualizar_fb_posts(token_g, page_token, historico=args.historico, start_date=start_date)
    print("FB Stories...")
    atualizar_fb_stories(token_g, page_token)
    print("IG Insights...")
    atualizar_ig(token_g, page_token, historico=args.historico, start_date=start_date)
    print("IG Posts...")
    atualizar_posts(token_g, page_token, historico=args.historico, start_date=start_date)
    print("IG Stories...")
    atualizar_ig_stories(token_g, page_token)
    print("Concluido.")


if __name__ == "__main__":
    main()

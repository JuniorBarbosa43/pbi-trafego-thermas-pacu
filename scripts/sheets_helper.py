"""
Helper para leitura e escrita no Google Sheets via API.
Usado por todos os scripts de atualizacao.
Nao requer dependencias externas alem de requests.

Suporta tres modos de escrita:
- overwrite: limpa tudo e escreve novos dados (modo legado)
- append: adiciona apenas linhas novas (para dados historicos)
- upsert: verifica por intervalo de data e atualiza dados recentes (para APIs com revisoes)
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import os
from datetime import datetime
from typing import List, Tuple


# Helpers para variáveis de ambiente
def _get_spreadsheet_id() -> str:
    """Lê SPREADSHEET_ID da variável de ambiente."""
    return os.getenv("SPREADSHEET_ID", "")


def obter_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Troca o refresh token por um access token valido."""
    data = urllib.parse.urlencode({
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
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


def sheets_request(method: str, url: str, token: str, body=None):
    """Faz uma requisicao autenticada para a Sheets API."""
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    if body:
        req.data = json.dumps(body).encode("utf-8")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def ler_dados(spreadsheet_id: str, sheet_name: str, token: str) -> List[List]:
    """
    Le todos os dados de uma aba (incluindo cabecalho).

    Args:
        spreadsheet_id: ID da planilha
        sheet_name: Nome da aba
        token: Access token

    Returns:
        Lista de listas (incluindo header na primeira linha)
    """
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
    range_str = f"{sheet_name}!A:ZZ"

    url = f"{base}/values/{urllib.parse.quote(range_str)}"
    result = sheets_request("GET", url, token)

    values = result.get("values", [])
    return values


def limpar_e_gravar(spreadsheet_id: str, sheet_name: str, headers: list, rows: list, token: str):
    """
    Limpa a aba e grava novos dados com cabecalho.
    Modo OVERWRITE (legado).

    Args:
        spreadsheet_id: ID da planilha
        sheet_name: Nome da aba
        headers: Lista com nomes das colunas
        rows: Lista de listas com os valores (sem cabecalho)
        token: Access token

    Returns:
        Numero total de celulas atualizadas
    """
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
    range_str = f"{sheet_name}!A1"

    # Prepara valores: cabecalho + dados
    values = [headers] + rows

    body = {
        "valueInputOption": "RAW",
        "data": [{
            "range": range_str,
            "values": values
        }]
    }

    # Clear primeiro
    sheets_request("POST", f"{base}/values:batchClear", token, {
        "ranges": [f"{sheet_name}!A:ZZ"]
    })

    # Grava
    url = f"{base}/values:batchUpdate"
    result = sheets_request("POST", url, token, body)
    total = result.get("totalUpdatedCells", 0)
    print(f"  Sheets '{sheet_name}': {len(rows)} linhas gravadas ({total} celulas) [OVERWRITE]")
    return total


def append_dados(spreadsheet_id: str, sheet_name: str, headers: list, rows: list, token: str) -> Tuple[int, int]:
    """
    Adiciona apenas linhas NOVAS que ja nao existem.
    Le dados existentes, compara e so adiciona o que falta.
    Modo APPEND (para dados historicos).

    Args:
        spreadsheet_id: ID da planilha
        sheet_name: Nome da aba
        headers: Lista com nomes das colunas
        rows: Lista de listas com novos dados (sem cabecalho)
        token: Access token

    Returns:
        Tupla (total_novas_linhas_adicionadas, total_linhas_existentes)
    """
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"

    # Le dados existentes
    existing_data = ler_dados(spreadsheet_id, sheet_name, token)

    if not existing_data:
        # Aba vazia, cria com cabecalho e todos os dados
        all_values = [headers] + rows
        range_str = f"{sheet_name}!A1"
        body = {
            "valueInputOption": "RAW",
            "data": [{
                "range": range_str,
                "values": all_values
            }]
        }
        url = f"{base}/values:batchUpdate"
        result = sheets_request("POST", url, token, body)
        total = result.get("totalUpdatedCells", 0)
        print(f"  Sheets '{sheet_name}': {len(rows)} linhas adicionadas ({total} celulas) [APPEND - aba vazia]")
        return (len(rows), 0)

    # Converte dados existentes para conjunto de tuplas para comparacao rapida
    # (ignora cabecalho se estiver presente)
    existing_rows = existing_data[1:] if existing_data else []
    existing_tuples = {tuple(row) for row in existing_rows}

    # Filtra apenas linhas novas
    new_rows = [row for row in rows if tuple(row) not in existing_tuples]

    if not new_rows:
        print(f"  Sheets '{sheet_name}': nenhuma linha nova encontrada [APPEND]")
        return (0, len(existing_rows))

    # Adiciona linhas novas ao final
    start_row = len(existing_rows) + 2  # +1 para header, +1 para proxima linha
    range_str = f"{sheet_name}!A{start_row}"

    body = {
        "valueInputOption": "RAW",
        "data": [{
            "range": range_str,
            "values": new_rows
        }]
    }

    url = f"{base}/values:batchUpdate"
    result = sheets_request("POST", url, token, body)
    total = result.get("totalUpdatedCells", 0)
    print(f"  Sheets '{sheet_name}': {len(new_rows)} linhas adicionadas ({total} celulas) [APPEND]")
    return (len(new_rows), len(existing_rows))


def upsert_por_data(spreadsheet_id: str, sheet_name: str, headers: list, rows: list,
                    token: str, key_cols: list = None) -> Tuple[int, int]:
    """
    Smart UPSERT: le dados existentes, identifica chaves de atualizacao,
    remove dados antigos para chaves sobrepostas, e escreve tudo.
    Ideal para APIs que revisam dados recentes (Meta, Google Ads, etc).

    Args:
        spreadsheet_id: ID da planilha
        sheet_name: Nome da aba
        headers: Lista com nomes das colunas
        rows: Lista de listas com novos dados (sem cabecalho)
        token: Access token
        key_cols: Lista de nomes de colunas que formam a chave unica (default: ["date_start"])

    Returns:
        Tupla (total_linhas_gravadas, total_linhas_antigas_removidas)
    """
    if key_cols is None:
        key_cols = ["date_start"]

    base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"

    # Le dados existentes
    existing_data = ler_dados(spreadsheet_id, sheet_name, token)

    if not existing_data:
        # Aba vazia, cria com cabecalho e todos os dados
        all_values = [headers] + rows
        range_str = f"{sheet_name}!A1"
        body = {
            "valueInputOption": "RAW",
            "data": [{
                "range": range_str,
                "values": all_values
            }]
        }
        url = f"{base}/values:batchUpdate"
        result = sheets_request("POST", url, token, body)
        total = result.get("totalUpdatedCells", 0)
        print(f"  Sheets '{sheet_name}': {len(rows)} linhas gravadas ({total} celulas) [UPSERT - aba vazia]")
        return (len(rows), 0)

    # Encontra indices das colunas chave
    existing_headers = existing_data[0] if existing_data else []
    try:
        new_headers = headers
        key_indices_new = [new_headers.index(col) for col in key_cols]
    except ValueError as e:
        print(f"  AVISO: coluna chave nao encontrada: {e}. Usando modo APPEND.")
        return append_dados(spreadsheet_id, sheet_name, headers, rows, token)

    # Extrai tuplas de chaves dos novos dados
    new_keys = set()
    for row in rows:
        try:
            key_tuple = tuple(row[idx] if idx < len(row) else "" for idx in key_indices_new)
            new_keys.add(key_tuple)
        except (IndexError, TypeError):
            pass

    # Encontra indices das colunas chave nos dados existentes
    try:
        key_indices_existing = [existing_headers.index(col) for col in key_cols]
    except ValueError:
        # Headers existentes incompativeis (aba antiga ou schema diferente)
        # Limpa a aba e reescreve do zero com os novos headers
        print(f"  AVISO: headers existentes incompativeis com key_cols={key_cols}. Reescrevendo aba do zero.")
        all_values = [headers] + rows
        sheets_request("POST", f"{base}/values:batchClear", token, {
            "ranges": [f"{sheet_name}!A:ZZ"]
        })
        range_str = f"{sheet_name}!A1"
        body = {
            "valueInputOption": "RAW",
            "data": [{"range": range_str, "values": all_values}]
        }
        result = sheets_request("POST", f"{base}/values:batchUpdate", token, body)
        total = result.get("totalUpdatedCells", 0)
        print(f"  Sheets '{sheet_name}': {len(rows)} linhas gravadas ({total} celulas) [REESCRITA - headers incompativeis]")
        return (len(rows), 0)

    # Filtra linhas antigas que NAO estao sendo atualizadas
    existing_rows = existing_data[1:] if len(existing_data) > 1 else []

    kept_rows = []
    removed_count = 0

    for row in existing_rows:
        try:
            existing_key = tuple(row[idx] if idx < len(row) else "" for idx in key_indices_existing)
            if existing_key not in new_keys:
                kept_rows.append(row)
            else:
                removed_count += 1
        except (IndexError, TypeError):
            kept_rows.append(row)

    # Combina dados: antigas (fora das chaves) + novas
    final_rows = kept_rows + rows
    all_values = [headers] + final_rows

    # Clear e reescreve tudo
    sheets_request("POST", f"{base}/values:batchClear", token, {
        "ranges": [f"{sheet_name}!A:ZZ"]
    })

    range_str = f"{sheet_name}!A1"
    body = {
        "valueInputOption": "RAW",
        "data": [{
            "range": range_str,
            "values": all_values
        }]
    }

    url = f"{base}/values:batchUpdate"
    result = sheets_request("POST", url, token, body)
    total = result.get("totalUpdatedCells", 0)
    print(f"  Sheets '{sheet_name}': {len(final_rows)} linhas gravadas ({total} celulas) [UPSERT - {removed_count} antigas removidas]")
    return (len(final_rows), removed_count)


def criar_sheet_se_nao_existe(spreadsheet_id: str, sheet_name: str, token: str):
    """
    Cria uma aba na planilha se ela nao existir.

    Args:
        spreadsheet_id: ID da planilha
        sheet_name: Nome da aba
        token: Access token
    """
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"

    # Verifica abas existentes
    info = sheets_request("GET", base, token)
    abas = [s["properties"]["title"] for s in info.get("sheets", [])]

    if sheet_name not in abas:
        body = {
            "requests": [{
                "addSheet": {
                    "properties": {"title": sheet_name}
                }
            }]
        }
        sheets_request("POST", f"{base}:batchUpdate", token, body)
        print(f"  Aba '{sheet_name}' criada.")
    else:
        print(f"  Aba '{sheet_name}' ja existe.")

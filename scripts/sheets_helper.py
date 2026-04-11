"""
Helper para leitura e escrita no Google Sheets via API.
Usado por todos os scripts de atualizacao.
Nao requer dependencias externas alem de requests.
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import os
from datetime import datetime


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


def limpar_e_gravar(spreadsheet_id: str, sheet_name: str, headers: list, rows: list, token: str):
    """
    Limpa a aba e grava novos dados com cabecalho.
    rows: lista de listas com os valores (sem cabecalho)
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
    print(f"  Sheets '{sheet_name}': {len(rows)} linhas gravadas ({total} celulas)")
    return total


def criar_sheet_se_nao_existe(spreadsheet_id: str, sheet_name: str, token: str):
    """Cria uma aba na planilha se ela nao existir."""
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

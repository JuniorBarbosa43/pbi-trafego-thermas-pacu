"""
Atualiza dados de ClimaTempo (Open-Meteo) no Google Sheets.
Usa API aberta Open-Meteo (sem autenticacao necessaria).
Localidade: Thermas Pacu (latitude=-22.9027, longitude=-49.6373)

Modo padrao: ultimos 14 dias + 14 dias de previsao
Modo --historico: dados desde 2025-01-01 ate hoje
"""

import json
import os
import sys
import argparse
import urllib.request
import urllib.parse
from datetime import date, timedelta
import time

sys.path.insert(0, os.path.dirname(__file__))
from sheets_helper import obter_access_token, upsert_por_data, criar_sheet_se_nao_existe

GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
SPREADSHEET_ID       = os.environ["SPREADSHEET_ID"]

SHEET_NAME = "ClimaTempo"

# Thermas Pacu coordinates
LAT = -22.9027
LON = -49.6373

HEADERS = [
    "Date", "temperature_max", "temperature_min", "temperature_mean",
    "precipitation_sum", "windspeed_max", "humidity_mean", "weathercode",
    "BomParaParque"
]


def fetch_historico(start_date: date, end_date: date) -> list:
    """Busca dados historicos via Open-Meteo Archive API."""
    print(f"  Buscando dados historicos {start_date} → {end_date}")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={LAT}&longitude={LON}"
        f"&start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
        f"precipitation_sum,windspeed_10m_max,relative_humidity_2m_mean,weathercode"
        f"&temperature_unit=celsius&windspeed_unit=kmh&timezone=America/Sao_Paulo"
    )

    try:
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())
            return data
    except Exception as e:
        print(f"    Erro ao buscar dados historicos: {e}")
        return {}


def fetch_forecast() -> dict:
    """Busca previsao dos proximos 16 dias via Open-Meteo Forecast API."""
    print(f"  Buscando previsao dos proximos 16 dias")

    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}&longitude={LON}"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
        f"precipitation_sum,windspeed_10m_max,relative_humidity_2m_mean,weathercode"
        f"&temperature_unit=celsius&windspeed_unit=kmh&timezone=America/Sao_Paulo"
    )

    try:
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())
            return data
    except Exception as e:
        print(f"    Erro ao buscar previsao: {e}")
        return {}


def process_daily_data(daily_data: dict, dates: list) -> list:
    """Processa dados diarios e retorna linhas para a planilha."""
    rows = []

    temp_max = daily_data.get("temperature_2m_max", [])
    temp_min = daily_data.get("temperature_2m_min", [])
    temp_mean = daily_data.get("temperature_2m_mean", [])
    precip = daily_data.get("precipitation_sum", [])
    wind_max = daily_data.get("windspeed_10m_max", [])
    humidity = daily_data.get("relative_humidity_2m_mean", [])
    weathercode = daily_data.get("weathercode", [])

    for i, d in enumerate(dates):
        t_max = temp_max[i] if i < len(temp_max) else 0
        t_min = temp_min[i] if i < len(temp_min) else 0
        t_mean = temp_mean[i] if i < len(temp_mean) else 0
        precip_val = precip[i] if i < len(precip) else 0
        wind = wind_max[i] if i < len(wind_max) else 0
        hum = humidity[i] if i < len(humidity) else 0
        weather = weathercode[i] if i < len(weathercode) else 0

        # BomParaParque: sem chuva (precip < 1mm) E temperatura >= 25C
        bom_para_parque = "Sim" if (precip_val < 1.0 and t_max >= 25) else "Nao"

        rows.append([
            d,
            round(float(t_max), 1) if t_max else "",
            round(float(t_min), 1) if t_min else "",
            round(float(t_mean), 1) if t_mean else "",
            round(float(precip_val), 1) if precip_val else "",
            round(float(wind), 1) if wind else "",
            round(float(hum), 1) if hum else "",
            int(weather) if weather else "",
            bom_para_parque,
        ])

    return rows


def main():
    parser = argparse.ArgumentParser(description="Atualiza ClimaTempo no Google Sheets")
    parser.add_argument("--historico", action="store_true", help="Fetch dados historicos desde 2025-01-01")
    args = parser.parse_args()

    print("=== ClimaTempo (Open-Meteo) → Google Sheets ===")

    token = obter_access_token(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    criar_sheet_se_nao_existe(SPREADSHEET_ID, SHEET_NAME, token)

    all_rows = []

    if args.historico:
        print("Modo HISTORICO: fetchando dados desde 2025-01-01")
        inicio = date(2025, 1, 1)
        fim = date.today()

        hist_data = fetch_historico(inicio, fim)
        if hist_data and "daily" in hist_data:
            dates = hist_data.get("daily", {}).get("time", [])
            daily = hist_data.get("daily", {})
            rows = process_daily_data(daily, dates)
            all_rows.extend(rows)
            print(f"  Registros historicos: {len(rows)}")

        time.sleep(0.5)
    else:
        # Modo padrao: ultimos 14 dias
        print("Modo INCREMENTAL: ultimos 14 dias")
        inicio = date.today() - timedelta(days=14)
        fim = date.today()

        hist_data = fetch_historico(inicio, fim)
        if hist_data and "daily" in hist_data:
            dates = hist_data.get("daily", {}).get("time", [])
            daily = hist_data.get("daily", {})
            rows = process_daily_data(daily, dates)
            all_rows.extend(rows)
            print(f"  Registros historicos (14 dias): {len(rows)}")

        time.sleep(0.5)

    # Sempre busca previsao (16 dias)
    print("Buscando previsao...")
    forecast_data = fetch_forecast()
    if forecast_data and "daily" in forecast_data:
        dates = forecast_data.get("daily", {}).get("time", [])
        daily = forecast_data.get("daily", {})
        rows = process_daily_data(daily, dates)
        all_rows.extend(rows)
        print(f"  Registros previsao: {len(rows)}")

    print(f"Total registros: {len(all_rows)}")

    # Upsert por Date
    upsert_por_data(SPREADSHEET_ID, SHEET_NAME, HEADERS, all_rows, token, key_cols=["Date"])

    print("Concluido.")


if __name__ == "__main__":
    main()

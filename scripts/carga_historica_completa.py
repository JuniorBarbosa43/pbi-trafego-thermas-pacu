"""
Carga Historica Completa
========================
Script master que executa todos os carregamentos de dados historicos em sequencia.
Roda cada script com flag --historico para buscar dados desde 2025-01-01 ate hoje.

Uso:
  python scripts/carga_historica_completa.py

Escripts executados:
  1. atualizar_meta_ads_sheets.py --historico
  2. atualizar_google_ads_sheets.py --historico
  3. atualizar_meta_organico_sheets.py --historico
  4. atualizar_whatsapp_sheets.py --historico
  5. atualizar_clima_sheets.py --historico
"""

import os
import sys
import subprocess
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    "atualizar_meta_ads_sheets.py",
    "atualizar_google_ads_sheets.py",
    "atualizar_meta_organico_sheets.py",
    "atualizar_whatsapp_sheets.py",
    "atualizar_clima_sheets.py",
]


def log(msg: str):
    """Imprime mensagem com timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def main():
    log("=" * 70)
    log("CARGA HISTORICA COMPLETA - Iniciando")
    log("=" * 70)

    total_inicio = datetime.now()
    resultados = []

    for script_name in SCRIPTS:
        script_path = os.path.join(SCRIPTS_DIR, script_name)

        if not os.path.exists(script_path):
            log(f"AVISO: {script_name} nao encontrado em {script_path}")
            resultados.append((script_name, "SKIP", "arquivo nao encontrado"))
            continue

        log("")
        log(f"Executando: {script_name}")
        log("-" * 70)

        inicio = datetime.now()
        try:
            result = subprocess.run(
                [sys.executable, script_path, "--historico"],
                cwd=SCRIPTS_DIR,
                capture_output=True,
                text=True,
                timeout=3600
            )

            duracao = (datetime.now() - inicio).total_seconds()

            if result.returncode == 0:
                log(f"{script_name} concluido com sucesso ({duracao:.1f}s)")
                resultados.append((script_name, "OK", f"{duracao:.1f}s"))
                if result.stdout:
                    for linha in result.stdout.splitlines()[-5:]:
                        log(f"  {linha}")
            else:
                log(f"ERRO ao executar {script_name}")
                resultados.append((script_name, "ERRO", result.returncode))
                if result.stdout:
                    log("STDOUT:")
                    for linha in result.stdout.splitlines()[-10:]:
                        log(f"  {linha}")
                if result.stderr:
                    log("STDERR:")
                    for linha in result.stderr.splitlines()[-10:]:
                        log(f"  {linha}")

        except subprocess.TimeoutExpired:
            log(f"TIMEOUT ao executar {script_name} (>3600s)")
            resultados.append((script_name, "TIMEOUT", ""))
        except Exception as e:
            log(f"EXCECAO ao executar {script_name}: {e}")
            resultados.append((script_name, "EXCECAO", str(e)))

    # Resumo final
    duracao_total = (datetime.now() - total_inicio).total_seconds()
    log("")
    log("=" * 70)
    log("RESUMO FINAL")
    log("=" * 70)

    for script_name, status, detalhe in resultados:
        log(f"{script_name}: {status} ({detalhe})")

    log("")
    log(f"Duracao total: {duracao_total:.1f}s ({duracao_total/60:.1f}m)")
    log("=" * 70)

    # Conta sucessos e erros
    sucessos = sum(1 for _, status, _ in resultados if status == "OK")
    erros = sum(1 for _, status, _ in resultados if status in ("ERRO", "TIMEOUT", "EXCECAO"))
    skips = sum(1 for _, status, _ in resultados if status == "SKIP")

    log(f"Resultado: {sucessos} OK, {erros} ERRO, {skips} SKIP")

    if erros > 0:
        sys.exit(1)
    else:
        log("Carga historica completa finalizada com sucesso.")
        sys.exit(0)


if __name__ == "__main__":
    main()

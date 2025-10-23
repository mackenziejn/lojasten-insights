from src.db_utils import criar_tabela, inserir_linha, ensure_store_sellers_from_df, logger
from src.etl import carregar_dados, tratar_dados
from src.validacao import corrigir_linha, validar_linha
from src.gerador_dados import gerar_dados_fake
from src.pipeline import executar_pipeline
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import os
import datetime
import shutil
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ----------------------------
# Configura pastas necessárias
# ----------------------------
for pasta in ["data/raw", "data/archived", "data/processed", "data/reports"]:
    os.makedirs(pasta, exist_ok=True)

import argparse
from logging.handlers import RotatingFileHandler


# ----------------------------
# Logging: rotating file handler (batch) + console
# ----------------------------
LOG_DIR = os.path.join('data', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
batch_log = os.path.join(LOG_DIR, 'batch.log')
web_log = os.path.join(LOG_DIR, 'web.log')

# configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(batch_log) for h in root_logger.handlers):
    rfh = RotatingFileHandler(batch_log, maxBytes=5_000_000, backupCount=5, encoding='utf-8')
    rfh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    root_logger.addHandler(rfh)

# console handler
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
root_logger.addHandler(ch)


# ----------------------------
# Função para arquivar CSV
# ----------------------------
def arquivar_csv(origem="data/raw/vendas.csv"):
    hoje = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = f"data/archived/vendas_{hoje}.csv"
    if os.path.exists(origem):
        shutil.move(origem, destino)
        logger.info("CSV archived: %s", destino)
    else:
        logger.warning("File not found for archive: %s", origem)

# ----------------------------
# Função para gerar PDF de relatório
# ----------------------------
def gerar_pdf_relatorio(resumo_path, relatorio_completo_path, pdf_path):
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    y = height - 50

    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "Relatório de Qualidade dos Dados")
    y -= 30

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Resumo do Relatório:")
    y -= 20

    resumo_df = pd.read_csv(resumo_path)
    for col in resumo_df.columns:
        valor = resumo_df[col].iloc[0]
        c.setFont("Helvetica", 11)
        c.drawString(60, y, f"{col.replace('_', ' ').capitalize()}: {valor}")
        y -= 15

    y -= 20
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Erros por linha (amostra):")
    y -= 20

    relatorio_df = pd.read_csv(relatorio_completo_path)
    if not relatorio_df.empty:
        amostra = relatorio_df[['cpf', 'telefone', 'data_nascimento', 'data_compra', 'erros']].head(10)
        for _, row in amostra.iterrows():
            linha = f"CPF: {row['cpf']} | Tel: {row['telefone']} | Nasc: {row['data_nascimento']} | Compra: {row['data_compra']} | Erros: {row['erros']}"
            c.setFont("Helvetica", 10)
            c.drawString(60, y, linha)
            y -= 12
            if y < 50:
                c.showPage()
                y = height - 50

    c.save()
    logger.info("PDF generated at: %s", pdf_path)

# ----------------------------
# Função principal
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description='ETL CLI for Vendas_teste')
    sub = parser.add_subparsers(dest='cmd')

    p_run = sub.add_parser('run', help='Run the pipeline processing data/raw/vendas.csv')
    p_run.add_argument('--generate-sample', action='store_true')
    p_run.add_argument('--sample-size', type=int, default=100)

    p_gen = sub.add_parser('generate-sample', help='Generate a sample vendas.csv')
    p_gen.add_argument('--sample-size', type=int, default=100)

    p_mig = sub.add_parser('migrate', help='Run DB migrations (idempotent)')

    p_dry = sub.add_parser('dry-run', help='Run pipeline validation without DB writes')

    args = parser.parse_args()

    if args.cmd is None:
        parser.print_help()
        return

    logger.info('CLI command: %s', args.cmd)

    if args.cmd == 'generate-sample':
        logger.info('Generating sample data: %d rows', args.sample_size)
        gerar_dados_fake('data/raw/vendas.csv', quantidade=args.sample_size)
        return

    if args.cmd == 'migrate':
        logger.info('Running migrations (schema)')
        criar_tabela()
        return

    # run or dry-run both need to load and treat data
    if args.cmd in ('run', 'dry-run'):
        if args.cmd == 'run' and getattr(args, 'generate_sample', False):
            logger.info('Generating sample data: %d rows', args.sample_size)
            gerar_dados_fake('data/raw/vendas.csv', quantidade=args.sample_size)

        criar_tabela()
        df = carregar_dados('data/raw/vendas.csv')
        if df.empty:
            logger.error('CSV is empty or malformed: data/raw/vendas.csv')
            return
        df = tratar_dados(df)

        if args.cmd == 'dry-run':
            logger.info('Running dry-run: validating %d rows', len(df))
            # only validate and report issues
            problemas = []
            for _, row in df.iterrows():
                r = corrigir_linha(row)
                erros = validar_linha(r)
                if erros:
                    problemas.append({'row': r.to_dict(), 'erros': erros})
            logger.info('Dry-run completed: %d rows with issues', len(problemas))
            return

        # normal run
        result = executar_pipeline(df, enviar_dropbox=False, caminho_raw='data/raw/vendas.csv')
        logger.info('Pipeline result: %s', result)
        arquivar_csv()

if __name__ == "__main__":
    main()

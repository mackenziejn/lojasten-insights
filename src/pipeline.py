import pandas as pd
import datetime
import os
from src.validacao import corrigir_linha, validar_linha
from src.db_utils import inserir_linha
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

def validar_e_padronizar_csv(df):
    """Valida estrutura do CSV e padroniza colunas obrigatórias"""

    # Colunas obrigatórias esperadas (ordem padrão)
    colunas_obrigatorias = [
        'id_cliente', 'nome_cliente', 'data_nascimento', 'rg', 'cpf',
        'endereco', 'numero', 'complemento', 'bairro', 'cidade', 'estado', 'cep', 'telefone',
        'codigo_produto', 'nome_produto', 'quantidade', 'valor_produto',
        'forma_pagamento', 'codigo_loja', 'nome_loja', 'codigo_vendedor', 'nome_vendedor',
        'data_venda', 'data_compra', 'status_venda', 'observacoes'
    ]

    # Verificar se pelo menos as colunas críticas estão presentes
    colunas_criticas = ['nome_cliente', 'nome_produto', 'quantidade', 'valor_produto',
                       'nome_loja', 'nome_vendedor', 'data_venda']

    colunas_faltando = [col for col in colunas_criticas if col not in df.columns]
    if colunas_faltando:
        raise ValueError(f"Colunas críticas faltando no CSV: {', '.join(colunas_faltando)}")

    # Reordenar colunas na ordem padrão (usar apenas as que existem)
    colunas_existentes = [col for col in colunas_obrigatorias if col in df.columns]
    df_padronizado = df[colunas_existentes].copy()

    # Adicionar colunas opcionais faltantes com valores padrão
    for col in colunas_obrigatorias:
        if col not in df_padronizado.columns:
            if col in ['quantidade', 'valor_produto']:
                df_padronizado[col] = 0
            elif col == 'status_venda':
                df_padronizado[col] = 'CONCLUIDA'
            else:
                df_padronizado[col] = ''

    return df_padronizado

def gerar_pdf_relatorio(resumo_path, relatorio_completo_path, pdf_path):
    """Gera um PDF resumindo a qualidade dos dados."""
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

def executar_pipeline(df, enviar_dropbox=False, caminho_raw="data/raw/vendas.csv", chunk_size=1000):
    """
    Executa o pipeline completo com processamento em chunks para economia de memória:
    - Corrige e valida os dados
    - Insere no banco
    - Gera relatório CSV e PDF
    - Retorna caminhos dos relatórios
    """
    os.makedirs("data/reports", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/archived", exist_ok=True)

    colunas_esperadas = [
        "id_cliente", "nome_cliente", "data_nascimento", "rg", "cpf", "endereco",
        "bairro", "cidade", "estado", "cep", "telefone",
        "codigo_produto", "nome_produto", "quantidade", "valor_produto", "forma_pagamento",
        "codigo_loja", "nome_loja", "codigo_vendedor", "nome_vendedor"
    ]

    inseridos = 0
    linhas_corrigidas = []
    erros_gerais = {}

    # Processar em chunks para reduzir uso de memória
    total_rows = len(df)
    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        chunk_df = df.iloc[start_idx:end_idx].copy()

        for _, row in chunk_df.iterrows():
            row = corrigir_linha(row)
            erros = validar_linha(row)

            row_dict = row.to_dict()
            row_dict['erros'] = ", ".join(erros) if erros else ""

            for erro in erros:
                erros_gerais[erro] = erros_gerais.get(erro, 0) + 1

            # inserir_linha now expects a dict with column keys
            inserir_linha(row_dict)
            inseridos += 1

            # Manter apenas últimas 1000 linhas corrigidas na memória para relatório
            linhas_corrigidas.append(row_dict)
            if len(linhas_corrigidas) > 1000:
                linhas_corrigidas.pop(0)

    data_stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Criar DataFrame apenas com amostra para relatório (últimas 1000 linhas)
    df_corrigido = pd.DataFrame(linhas_corrigidas)
    relatorio_completo_path = f"data/reports/vendas_corrigido_{data_stamp}.csv"
    df_corrigido.to_csv(relatorio_completo_path, index=False)

    # Resumo de erros
    resumo = {
        "total_processado": [total_rows],
        "registros_inseridos": [inseridos],
        "registros_com_erros": [sum(1 for r in linhas_corrigidas if r['erros'])]
    }
    for erro, qtd in erros_gerais.items():
        chave = f"erro_{erro.replace(' ', '_').lower()}"
        resumo[chave] = [qtd]

    df_resumo = pd.DataFrame(resumo)
    resumo_path = f"data/reports/resumo_qualidade_{data_stamp}.csv"
    df_resumo.to_csv(resumo_path, index=False)

    pdf_path = f"data/reports/relatorio_qualidade_{data_stamp}.pdf"
    gerar_pdf_relatorio(resumo_path, relatorio_completo_path, pdf_path)

    # Move CSV original para archived
    destino = None
    if os.path.exists(caminho_raw):
        destino = f"data/archived/vendas_{data_stamp}.csv"
        os.rename(caminho_raw, destino)

    # Salva CSV processado (apenas amostra para evitar estouro de memória)
    caminho_processed = f"data/processed/vendas_tratado_{data_stamp}.csv"
    df_corrigido.to_csv(caminho_processed, index=False)

    link_publico = None
    if enviar_dropbox:
        # Implementar lógica para enviar para Dropbox e obter link público
        # Por enquanto, retornar None
        pass

    return {
        "resumo_csv": resumo_path,
        "relatorio_csv": relatorio_completo_path,
        "pdf": pdf_path,
        "csv_processado": caminho_processed,
        "csv_arquivado": destino,
        "link_publico": link_publico
    }

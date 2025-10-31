# src/pipeline.py
import pandas as pd
import datetime
import os
from src.validacao import corrigir_linha, validar_linha
from src.db_utils import inserir_linha, ensure_store_sellers_from_df, get_db_connection
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import logging
import json
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def validar_e_padronizar_csv(df):
    """Valida estrutura do CSV e padroniza colunas obrigatórias"""

    # Colunas obrigatórias esperadas
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
    try:
        c = canvas.Canvas(pdf_path, pagesize=A4)
        width, height = A4
        y = height - 50

        # Título
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y, "Relatório de Qualidade dos Dados")
        y -= 30

        # Data do relatório
        c.setFont("Helvetica", 10)
        c.drawString(50, y, f"Gerado em: {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
        y -= 30

        # Resumo
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, "Resumo do Processamento:")
        y -= 20

        # Ler resumo se existir
        if os.path.exists(resumo_path):
            try:
                resumo_df = pd.read_csv(resumo_path)
                for col in resumo_df.columns:
                    valor = resumo_df[col].iloc[0]
                    c.setFont("Helvetica", 10)
                    c.drawString(60, y, f"{col.replace('_', ' ').title()}: {valor}")
                    y -= 15
                    if y < 100:
                        c.showPage()
                        y = height - 50
            except Exception as e:
                c.setFont("Helvetica", 10)
                c.drawString(60, y, f"Erro ao ler resumo: {e}")
                y -= 15
        else:
            c.setFont("Helvetica", 10)
            c.drawString(60, y, "Resumo não disponível")
            y -= 15

        y -= 20

        # Amostra de erros
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, "Amostra de Erros por Linha:")
        y -= 20

        # Ler relatório completo se existir
        if os.path.exists(relatorio_completo_path):
            try:
                relatorio_df = pd.read_csv(relatorio_completo_path)
                if not relatorio_df.empty and 'erros' in relatorio_df.columns:
                    # Filtrar apenas linhas com erros
                    linhas_com_erros = relatorio_df[relatorio_df['erros'].notna() & (relatorio_df['erros'] != '')]
                    
                    if not linhas_com_erros.empty:
                        amostra = linhas_com_erros.head(8)  # Limitar a 8 linhas para o PDF
                        
                        for _, row in amostra.iterrows():
                            # Preparar texto da linha
                            info_parts = []
                            if 'cpf' in row and pd.notna(row['cpf']):
                                info_parts.append(f"CPF: {row['cpf']}")
                            if 'nome_cliente' in row and pd.notna(row['nome_cliente']):
                                info_parts.append(f"Cliente: {row['nome_cliente'][:20]}...")
                            
                            linha_info = " | ".join(info_parts)
                            erros_text = f"Erros: {row['erros']}"
                            
                            # Desenhar informações da linha
                            c.setFont("Helvetica", 8)
                            c.drawString(60, y, linha_info)
                            y -= 12
                            
                            # Desenhar erros (pode quebrar linha)
                            erros_parts = [erros_text[i:i+80] for i in range(0, len(erros_text), 80)]
                            for part in erros_parts:
                                c.drawString(70, y, part)
                                y -= 10
                            
                            y -= 5  # Espaço entre linhas
                            
                            if y < 50:
                                c.showPage()
                                y = height - 50
                    else:
                        c.setFont("Helvetica", 10)
                        c.drawString(60, y, "Nenhum erro encontrado nos dados")
                        y -= 15
            except Exception as e:
                c.setFont("Helvetica", 10)
                c.drawString(60, y, f"Erro ao ler relatório: {e}")
                y -= 15
        else:
            c.setFont("Helvetica", 10)
            c.drawString(60, y, "Relatório completo não disponível")
            y -= 15

        # Rodapé
        c.showPage()
        c.setFont("Helvetica", 8)
        c.drawString(50, 30, "Relatório gerado automaticamente pelo Sistema LojasTen Insights")

        c.save()
        logger.info(f"✅ PDF gerado: {pdf_path}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar PDF: {e}")

def preparar_dados_para_insercao(row):
    """
    Prepara os dados da linha para inserção no banco.
    Converte para o formato esperado pela função inserir_linha.
    """
    # Mapeamento de colunas esperadas
    mapeamento_colunas = {
        'id_cliente': 'id_cliente',
        'nome_cliente': 'nome_cliente', 
        'data_nascimento': 'data_nascimento',
        'rg': 'rg',
        'cpf': 'cpf',
        'endereco': 'endereco',
        'numero': 'numero',
        'complemento': 'complemento',
        'bairro': 'bairro',
        'cidade': 'cidade',
        'estado': 'estado',
        'cep': 'cep',
        'telefone': 'telefone',
        'codigo_produto': 'codigo_produto',
        'nome_produto': 'nome_produto',
        'quantidade': 'quantidade',
        'valor_produto': 'valor_produto',
        'data_venda': 'data_venda',
        'data_compra': 'data_compra',
        'forma_pagamento': 'forma_pagamento',
        'codigo_loja': 'codigo_loja',
        'nome_loja': 'nome_loja',
        'codigo_vendedor': 'codigo_vendedor',
        'nome_vendedor': 'nome_vendedor'
    }
    
    dados_insercao = {}
    
    for coluna_origem, coluna_destino in mapeamento_colunas.items():
        if coluna_origem in row:
            valor = row[coluna_origem]
            # Tratar valores NaN/None
            if pd.isna(valor) or valor is None:
                valor = ''
            dados_insercao[coluna_destino] = valor
        else:
            # Valor padrão para colunas obrigatórias
            if coluna_destino in ['quantidade', 'valor_produto']:
                dados_insercao[coluna_destino] = 0
            elif coluna_destino == 'data_compra' and 'data_venda' in dados_insercao:
                dados_insercao[coluna_destino] = dados_insercao['data_venda']
            else:
                dados_insercao[coluna_destino] = ''
    
    return dados_insercao

def processar_chunk(df_chunk, start_idx):
    """Processa um chunk de dados e retorna estatísticas"""
    linhas_corrigidas = []
    erros_chunk = {}
    inseridos_chunk = 0
    erros_insercao_chunk = 0
    
    for idx, row in df_chunk.iterrows():
        try:
            # Corrigir e validar linha
            row_corrigida = corrigir_linha(row)
            erros = validar_linha(row_corrigida)

            # Preparar dados para inserção
            dados_insercao = preparar_dados_para_insercao(row_corrigida)
            dados_insercao['erros'] = ", ".join(erros) if erros else ""

            # Contar erros
            for erro in erros:
                erros_chunk[erro] = erros_chunk.get(erro, 0) + 1

            # Inserir no banco
            sucesso_insercao = inserir_linha(dados_insercao)
            if sucesso_insercao:
                inseridos_chunk += 1
            else:
                erros_insercao_chunk += 1

            # Manter para relatório
            linha_relatorio = dados_insercao.copy()
            linha_relatorio['indice_original'] = idx
            linhas_corrigidas.append(linha_relatorio)

        except Exception as e:
            erros_insercao_chunk += 1
            logger.error(f"❌ Erro ao processar linha {idx}: {e}")
            continue
    
    return linhas_corrigidas, erros_chunk, inseridos_chunk, erros_insercao_chunk

def executar_pipeline(df, enviar_dropbox=False, caminho_raw="data/raw/vendas.csv", chunk_size=500):
    """
    Executa o pipeline completo com processamento em chunks para economia de memória:
    - Corrige e valida os dados
    - Insere no banco
    - Gera relatório CSV e PDF
    - Retorna caminhos dos relatórios
    """
    try:
        # Criar diretórios necessários
        os.makedirs("data/reports", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)
        os.makedirs("data/archived", exist_ok=True)

        logger.info("🚀 Iniciando pipeline de processamento...")
        logger.info(f"📊 Total de linhas para processar: {len(df)}")
        logger.info(f"🔢 Tamanho do chunk: {chunk_size}")

        # Primeiro, garantir que lojas e vendedores existem no banco
        logger.info("🔄 Sincronizando lojas e vendedores com o banco...")
        ensure_store_sellers_from_df(df)

        # Variáveis para estatísticas
        total_linhas = len(df)
        todas_linhas_corrigidas = []
        todos_erros = {}
        total_inseridos = 0
        total_erros_insercao = 0

        # Processar em chunks para reduzir uso de memória
        total_chunks = (total_linhas + chunk_size - 1) // chunk_size
        
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min(start_idx + chunk_size, total_linhas)
            
            logger.info(f"📦 Processando chunk {chunk_num + 1}/{total_chunks} (linhas {start_idx}-{end_idx})...")
            
            df_chunk = df.iloc[start_idx:end_idx].copy()
            
            linhas_corrigidas, erros_chunk, inseridos_chunk, erros_insercao_chunk = processar_chunk(df_chunk, start_idx)
            
            # Acumular resultados
            todas_linhas_corrigidas.extend(linhas_corrigidas)
            for erro, count in erros_chunk.items():
                todos_erros[erro] = todos_erros.get(erro, 0) + count
            total_inseridos += inseridos_chunk
            total_erros_insercao += erros_insercao_chunk
            
            logger.info(f"   ✅ Chunk {chunk_num + 1} processado: {inseridos_chunk} inseridos, {erros_insercao_chunk} erros")

        # Gerar relatórios
        data_stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Criar DataFrame com amostra para relatório (últimas 1000 linhas)
        amostra_relatorio = todas_linhas_corrigidas[-1000:] if len(todas_linhas_corrigidas) > 1000 else todas_linhas_corrigidas
        
        if amostra_relatorio:
            df_relatorio = pd.DataFrame(amostra_relatorio)
            relatorio_completo_path = f"data/reports/vendas_corrigido_{data_stamp}.csv"
            df_relatorio.to_csv(relatorio_completo_path, index=False, encoding='utf-8')
            logger.info(f"✅ Relatório completo salvo: {relatorio_completo_path}")
        else:
            relatorio_completo_path = None
            logger.warning("⚠️ Nenhum dado para gerar relatório completo")

        # Resumo de processamento
        resumo = {
            "total_processado": [total_linhas],
            "registros_inseridos": [total_inseridos],
            "erros_insercao": [total_erros_insercao],
            "registros_com_erros_validacao": [sum(1 for r in todas_linhas_corrigidas if r.get('erros', ''))],
            "taxa_sucesso": [f"{(total_inseridos/total_linhas*100):.1f}%"] if total_linhas > 0 else ["0%"],
            "total_chunks_processados": [total_chunks],
            "data_processamento": [datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")]
        }
        
        # Adicionar contagem de erros por tipo
        for erro, qtd in todos_erros.items():
            chave = f"erro_{erro.replace(' ', '_').replace('/', '_').lower()}"
            resumo[chave] = [qtd]

        df_resumo = pd.DataFrame(resumo)
        resumo_path = f"data/reports/resumo_qualidade_{data_stamp}.csv"
        df_resumo.to_csv(resumo_path, index=False, encoding='utf-8')
        logger.info(f"✅ Resumo salvo: {resumo_path}")

        # Gerar PDF
        pdf_path = f"data/reports/relatorio_qualidade_{data_stamp}.pdf"
        gerar_pdf_relatorio(resumo_path, relatorio_completo_path, pdf_path)

        # Mover CSV original para archived se existir
        destino = None
        if os.path.exists(caminho_raw):
            destino = f"data/archived/vendas_{data_stamp}.csv"
            try:
                os.rename(caminho_raw, destino)
                logger.info(f"✅ CSV original arquivado: {destino}")
            except Exception as e:
                logger.error(f"❌ Erro ao arquivar CSV original: {e}")
                destino = None

        # Salvar CSV processado (apenas amostra)
        caminho_processed = None
        if amostra_relatorio:
            caminho_processed = f"data/processed/vendas_tratado_{data_stamp}.csv"
            df_relatorio.to_csv(caminho_processed, index=False, encoding='utf-8')
            logger.info(f"✅ CSV processado salvo: {caminho_processed}")

        link_publico = None
        if enviar_dropbox:
            # TODO: Implementar lógica para enviar para Dropbox
            logger.info("📤 Upload para Dropbox (não implementado)")
            # from src.dropbox_upload import upload_to_dropbox
            # link_publico = upload_to_dropbox(pdf_path)

        # Log final
        logger.info(f"🎉 Pipeline concluído com sucesso!")
        logger.info(f"📈 Estatísticas finais:")
        logger.info(f"   • Total processado: {total_linhas}")
        logger.info(f"   • Registros inseridos: {total_inseridos}")
        logger.info(f"   • Erros de inserção: {total_erros_insercao}")
        logger.info(f"   • Taxa de sucesso: {resumo['taxa_sucesso'][0]}")
        logger.info(f"   • Chunks processados: {total_chunks}")

        return {
            "sucesso": True,
            "resumo_csv": resumo_path,
            "relatorio_csv": relatorio_completo_path,
            "pdf": pdf_path,
            "csv_processado": caminho_processed,
            "csv_arquivado": destino,
            "link_publico": link_publico,
            "estatisticas": {
                "total_processado": total_linhas,
                "inseridos": total_inseridos,
                "erros_insercao": total_erros_insercao,
                "erros_validacao": len(todos_erros),
                "taxa_sucesso": resumo['taxa_sucesso'][0]
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Erro fatal no pipeline: {e}")
        import traceback
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        
        return {
            "sucesso": False,
            "erro": str(e),
            "resumo_csv": None,
            "relatorio_csv": None,
            "pdf": None,
            "csv_processado": None,
            "csv_arquivado": None,
            "link_publico": None,
            "estatisticas": {}
        }

def executar_pipeline_simples(df):
    """
    Versão simplificada do pipeline para testes rápidos
    """
    logger.info("🚀 Executando pipeline simplificado...")
    
    # Processar apenas as primeiras 50 linhas para teste
    df_amostra = df.head(50).copy()
    
    resultado = executar_pipeline(
        df_amostra, 
        enviar_dropbox=False,
        chunk_size=10
    )
    
    return resultado

def validar_estrutura_csv(df):
    """
    Valida a estrutura básica do CSV antes do processamento
    """
    colunas_obrigatorias = ['nome_cliente', 'nome_produto', 'quantidade', 'valor_produto', 'data_venda']
    
    colunas_faltando = [col for col in colunas_obrigatorias if col not in df.columns]
    if colunas_faltando:
        raise ValueError(f"Colunas obrigatórias faltando: {', '.join(colunas_faltando)}")
    
    # Verificar se há dados
    if df.empty:
        raise ValueError("DataFrame vazio - nenhum dado para processar")
    
    # Verificar tipos básicos
    if 'quantidade' in df.columns:
        try:
            df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(0).astype(int)
        except:
            raise ValueError("Erro ao converter coluna 'quantidade' para numérico")
    
    if 'valor_produto' in df.columns:
        try:
            df['valor_produto'] = pd.to_numeric(df['valor_produto'], errors='coerce').fillna(0.0)
        except:
            raise ValueError("Erro ao converter coluna 'valor_produto' para numérico")
    
    logger.info("✅ Estrutura do CSV validada com sucesso")
    return df

def diagnosticar_pipeline(df):
    """
    Executa diagnóstico do pipeline sem processar dados
    """
    logger.info("🔍 Executando diagnóstico do pipeline...")
    
    # 1. Verificar estrutura do DataFrame
    try:
        df_validado = validar_estrutura_csv(df)
        logger.info("✅ Estrutura de dados: OK")
    except Exception as e:
        logger.error(f"❌ Estrutura de dados: {e}")
        return False
    
    # 2. Verificar conexão com banco
    try:
        conn, db_type = get_db_connection()
        logger.info(f"✅ Conexão com banco: OK ({db_type})")
        conn.close()
    except Exception as e:
        logger.error(f"❌ Conexão com banco: {e}")
        return False
    
    # 3. Verificar funções de validação
    try:
        from src.validacao import corrigir_linha, validar_linha
        linha_teste = df.iloc[0] if not df.empty else None
        if linha_teste is not None:
            linha_corrigida = corrigir_linha(linha_teste)
            erros = validar_linha(linha_corrigida)
            logger.info(f"✅ Funções de validação: OK (erros na linha teste: {len(erros)})")
    except Exception as e:
        logger.error(f"❌ Funções de validação: {e}")
        return False
    
    # 4. Verificar diretórios
    diretorios = ["data/reports", "data/processed", "data/archived"]
    for diretorio in diretorios:
        if not os.path.exists(diretorio):
            try:
                os.makedirs(diretorio)
                logger.info(f"✅ Diretório criado: {diretorio}")
            except:
                logger.error(f"❌ Não foi possível criar diretório: {diretorio}")
                return False
        else:
            logger.info(f"✅ Diretório existe: {diretorio}")
    
    logger.info("🎉 Diagnóstico concluído - Pipeline pronto para uso!")
    return True

if __name__ == "__main__":
    # Teste básico do pipeline
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline de processamento de dados')
    parser.add_argument('--csv', type=str, help='Caminho para o arquivo CSV')
    parser.add_argument('--simples', action='store_true', help='Executar pipeline simplificado')
    parser.add_argument('--diagnostico', action='store_true', help='Executar apenas diagnóstico')
    
    args = parser.parse_args()
    
    if args.csv:
        try:
            df = pd.read_csv(args.csv)
            logger.info(f"📊 CSV carregado: {len(df)} linhas")
            
            if args.diagnostico:
                diagnosticar_pipeline(df)
            elif args.simples:
                resultado = executar_pipeline_simples(df)
                logger.info(f"✅ Pipeline simplificado executado: {resultado}")
            else:
                resultado = executar_pipeline(df)
                logger.info(f"✅ Pipeline completo executado: {resultado}")
                
        except Exception as e:
            logger.error(f"❌ Erro ao executar pipeline: {e}")
    else:
        logger.info("💡 Uso: python pipeline.py --csv caminho/arquivo.csv [--simples|--diagnostico]")
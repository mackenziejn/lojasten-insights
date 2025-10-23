import pandas as pd
import os

def carregar_dados(caminho_csv="data/raw/vendas.csv"):
    """
    Carrega os dados do CSV original.
    Retorna um DataFrame vazio se o arquivo não existir.
    """
    if not os.path.exists(caminho_csv):
        print(f"❌ Arquivo não encontrado em {caminho_csv}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(caminho_csv, sep=";", dtype=str)
        print(f"✅ CSV carregado com sucesso: {caminho_csv}")
        return df
    except Exception as e:
        print(f"❌ Erro ao carregar CSV: {e}")
        return pd.DataFrame()


def tratar_dados(df):
    """
    Prepara o DataFrame:
    - Remove duplicatas
    - Garante colunas corretas
    - Ordena por id_cliente
    """
    if df.empty:
        return df

    # Remove duplicatas
    df = df.drop_duplicates()

    # Colunas esperadas
    colunas_esperadas = [
        "id_cliente", "nome_cliente", "data_nascimento", "rg", "cpf", "endereco",
        "numero", "complemento", "bairro", "cidade", "estado", "cep", "telefone",
        "codigo_produto", "nome_produto", "quantidade", "valor_produto", "forma_pagamento",
        "codigo_loja", "nome_loja", "codigo_vendedor", "nome_vendedor"
    ]

    # Adiciona colunas faltantes
    for col in colunas_esperadas:
        if col not in df.columns:
            df[col] = ""

    # Ordena por id_cliente
    if "id_cliente" in df.columns:
        df = df.sort_values(by="id_cliente")

    # Reordena colunas
    df = df[colunas_esperadas]

    print("✅ Dados tratados e colunas padronizadas.")
    return df

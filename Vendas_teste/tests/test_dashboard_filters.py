import pytest
import pandas as pd
from unittest.mock import patch
import sys
import os

import streamlit as st

# Initialize required session state keys for testing BEFORE importing app
if 'role' not in st.session_state:
    st.session_state['role'] = 'admin'
if 'permissions' not in st.session_state:
    st.session_state['permissions'] = {
        "ver_filtros": True,
        "ver_indicadores": True,
        "ver_graficos": True,
        "executar_pipeline": True,
        "analisar_todas_lojas": True,
        "upload_csv": True
    }
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = True
if 'usuario' not in st.session_state:
    st.session_state['usuario'] = 'test_user'

# Mock carregar_dados_sqlite to return a dummy dataframe before importing app
dummy_df = pd.DataFrame({
    "codigo_loja": ["L001"],
    "codigo_vendedor": ["V001"],
    "nome_vendedor": ["Test"],
    "nome_loja": ["Loja Test"],
    "nome_produto": ["Produto A"],
    "forma_pagamento": ["Cartao"],
    "valor_produto": [100],
    "data_venda_dt": pd.to_datetime(["2023-01-01"])
})

with patch('sys.modules', {'app': None}):
    with patch('app.carregar_dados_sqlite', return_value=dummy_df):
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dashboard')))
        import app

@pytest.fixture
def sample_data():
    data = {
        "codigo_loja": ["L001", "L001", "L002", "L002", "L002", "L003", "L003"],
        "codigo_vendedor": ["V001", "V002", "V003", "V004", "V007", "V005", "V006"],
        "nome_vendedor": ["Joao", "Carlos", "Mariana", "Fernanda", "Mackenzie Nogueira", "Thiago", "Barone"],
        "nome_loja": ["Loja Centro", "Loja Centro", "Loja Shopping", "Loja Shopping", "Loja Shopping", "Loja Bairro", "Loja Bairro"],
        "nome_produto": ["Produto A", "Produto B", "Produto A", "Produto C", "Produto B", "Produto D", "Produto E"],
        "forma_pagamento": ["Cartao", "Dinheiro", "Cartao", "Dinheiro", "Pix", "Cartao", "Dinheiro"],
        "valor_produto": [100, 200, 150, 300, 250, 400, 350],
        "data_venda_dt": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-03", "2023-01-04", "2023-01-02", "2023-01-05"])
    }
    return pd.DataFrame(data)

def test_vendedor_filter(sample_data):
    with patch('app.carregar_dados_sqlite', return_value=sample_data):
        df = app.carregar_dados_sqlite()
        for _, row in df.iterrows():
            assert row['codigo_loja'] in ["L001", "L002", "L003"]
            assert row['codigo_vendedor'] in ["V001", "V002", "V003", "V004", "V005", "V006", "V007"]
        filtered = df[df['nome_vendedor'] == "Joao"]
        assert all(filtered['codigo_vendedor'] == "V001")
        assert all(filtered['nome_loja'] == "Loja Centro")

def test_loja_vendedor_consistency(sample_data):
    loja_vendedor_map = {
        "L001": ["V001", "V002"],
        "L002": ["V003", "V004", "V007"],
        "L003": ["V005", "V006"]
    }
    for _, row in sample_data.iterrows():
        assert row['codigo_vendedor'] in loja_vendedor_map[row['codigo_loja']]

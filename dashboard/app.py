import sys
import os
import random
import unidecode
import pandas as pd
import plotly.express as px
import streamlit as st
import logging
import json
from logging.handlers import RotatingFileHandler

# Debug: verificar estrutura de arquivos
st.write("Diretório atual:", os.getcwd())
st.write("Arquivos no diretório:", os.listdir("."))
st.write("Arquivos em Vendas_teste:", os.listdir("Vendas_teste") if os.path.exists("Vendas_teste") else "Pasta não existe")
st.write("Arquivos em Vendas_teste/data:", os.listdir("Vendas_teste/data") if os.path.exists("Vendas_teste/data") else "Pasta não existe")

# 🔹 Adiciona o diretório pai de 'src' ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 🔹 Importações do seu projeto
from src.pipeline import executar_pipeline
from src.validacao import corrigir_linha, validar_linha
from src.db_utils import carregar_usuarios, salvar_usuario, deletar_usuario

# Configurações do dashboard
st.set_page_config(page_title="Painel de Vendas", layout="wide")
REPORTS_DIR = "data/reports"
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "db", "vendas.db"))
USERS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "users.json"))

# Carregar usuários do banco
usuarios = carregar_usuarios()

# Se não há usuários, carregar do JSON como fallback
if not usuarios:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE) as f:
            usuarios = json.load(f)
    else:
        # Criar usuários padrão se não existir
        default_users = {
            "admin": {
                "password": "senha123",
                "role": "admin",
                "nome": "Administrador",
                "loja": "Todas",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": True,
                    "upload_csv": True
                }
            },
            "csilva": {
                "password": "csilva1976",
                "role": "admin",
                "nome": "Carlos Silva",
                "loja": "Loja Centro",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            },
            "maoliveira": {
                "password": "maoliveira1980",
                "role": "user",
                "nome": "Maria Oliveira",
                "loja": "Loja Norte",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            },
            "josouza": {
                "password": "josouza1986",
                "role": "user",
                "nome": "João Souza",
                "loja": "Loja Sul",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            },
            "antonios": {
                "password": "antonios1977",
                "role": "admin",
                "nome": "Antonio Santos",
                "loja": "Loja Leste",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            },
            "baronem": {
                "password": "baronem1990",
                "role": "user",
                "nome": "Barbara Neme",
                "loja": "Loja Oeste",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            },
            "thiagoc": {
                "password": "thiagoc1991",
                "role": "admin",
                "nome": "Thiago Costa",
                "loja": "Loja Centro",
                "permissions": {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }
            }
        }
        
        with open(USERS_FILE, "w") as f:
            json.dump(default_users, f, indent=4)
        usuarios = default_users

# Configure web logger to separate Streamlit logs from batch logs
LOG_DIR = os.path.join('data', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
web_log = os.path.join(LOG_DIR, 'web.log')
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(web_log) for h in root_logger.handlers):
    wfh = RotatingFileHandler(web_log, maxBytes=2_000_000, backupCount=3, encoding='utf-8')
    wfh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    root_logger.addHandler(wfh)

# 🔹 Função para carregar dados do SQLite (otimizada para memória)
@st.cache_data
def carregar_dados_sqlite(limit=50000):
    """
    Carrega dados do SQLite com limite para evitar estouro de memória
    """
    from src.db_utils import buscar_vendas
    return buscar_vendas(limit=limit)

# 🔹 Função para obter lojas do banco de dados
@st.cache_data
def obter_lojas():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT nome_loja FROM lojas ORDER BY nome_loja"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['nome_loja'].tolist()

# 🔹 Função para adicionar espaçamento entre seções
def nova_linha():
    st.markdown("<br>", unsafe_allow_html=True)

# 🔹 Função para detectar separador automaticamente
def detectar_separador(uploaded_file):
    """Detecta automaticamente o separador do CSV (vírgula ou ponto e vírgula)"""
    # Ler primeiras linhas como texto para detectar separador
    sample = uploaded_file.read(1024).decode('utf-8')
    uploaded_file.seek(0)  # Resetar ponteiro do arquivo

    # Contar ocorrências de possíveis separadores
    comma_count = sample.count(',')
    semicolon_count = sample.count(';')

    # Retornar separador com mais ocorrências
    return ',' if comma_count >= semicolon_count else ';'

# 🔹 Função para validar e padronizar estrutura do CSV
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

# 🔹 Cabeçalho exibido somente antes da autenticação com logo
if not st.session_state.get("autenticado", False):
    from PIL import Image

    logo_path = os.path.join(os.path.dirname(__file__), "..", "assets", "logo.png")
    if os.path.exists(logo_path):
        logo = Image.open(logo_path)

        # Adicionar três linhas em branco acima da imagem usando HTML:
        st.markdown("<br><br><br>", unsafe_allow_html=True)

        # Criar 5 colunas
        col1, col2, col3, col4, col5 = st.columns(5)

        # Colocar a imagem na coluna central (col3) e aumentar tamanho
        with col3:
            st.image(logo, width=800, clamp=False)

    else:
        st.warning("Logo não encontrado no caminho especificado.")

    st.markdown(
    "<p style='text-align:center; color:#D1D5DB; font-size:26px; font-weight:bold; margin-top:10px;'>"
    "Insights estratégicos guiados por dashboards dinâmicos."
    "</p>",
    unsafe_allow_html=True
)

# 🔹 Autenticação
if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    with st.sidebar:
        st.markdown(
            "<h2 style='font-size:28px; font-weight:;'>Login</h2>",
            unsafe_allow_html=True
        )
        usuario = st.text_input("Usuário", key="usuario_input")
        senha = st.text_input("Senha", type="password", key="senha_input")
        if st.button("Entrar"):
            if usuario in usuarios and senha == usuarios[usuario]["password"]:
                # Verificar se usuário está ativo
                if not usuarios[usuario].get("ativo", True):
                    st.error("❌ Conta desativada. Entre em contato com o administrador.")
                    st.stop()

                st.session_state.autenticado = True
                st.session_state.usuario = usuario
                st.session_state.role = usuarios[usuario]["role"]
                st.session_state.permissions = usuarios[usuario]["permissions"]
                st.session_state.nome_usuario = usuarios[usuario]["nome"]
                st.session_state.loja_usuario = usuarios[usuario]["loja"]
                st.session_state.codigo_vendedor = usuarios[usuario].get("codigo_vendedor")
                st.success("✅ Autenticado com sucesso!")
                st.rerun()
            else:
                st.error("❌ Credenciais inválidas.")
                st.stop()
    st.markdown("<br><br>", unsafe_allow_html=True)
else:
    # 🔹 Engrenagem de configuração para admin - SÓ APARECE NO DASHBOARD, NÃO NA CONFIGURAÇÃO
    if st.session_state.role == "admin":
        if not st.session_state.get("show_config", False):
            if st.sidebar.button("⚙️", help="Configurações", key="config_button_main"):
                st.session_state.show_config = True
                st.rerun()
        else:
            st.session_state.show_config = True

    # Mensagem de boas-vindas - AGORA ABAIXO da engrenagem
    if not st.session_state.get("show_config", False):
        st.sidebar.success(f"Bem-vindo, {st.session_state.nome_usuario}!")
        st.sidebar.info(f"Insights: {st.session_state.loja_usuario}")

    # Botão Sair
    if not st.session_state.get("show_config", False):
        if st.sidebar.button("Sair", key="logout_button_main"):
            st.session_state.autenticado = False
            st.session_state.usuario = ""
            st.session_state.role = ""
            st.session_state.permissions = {}
            st.session_state.show_config = False
            st.session_state.nome_usuario = ""
            st.session_state.loja_usuario = ""
            st.rerun()

if not st.session_state.autenticado:
    st.stop()

# 🔹 Seção de Configuração
if st.session_state.get("show_config", False):
    st.header("Configuração de Usuários")
    
    # 🔹 SEÇÃO PARA ADICIONAR NOVO USUÁRIO
    st.markdown("---")
    st.subheader("➕ Adicionar Novo Usuário")
    
    # Verificar login fora do form
    col_check1, col_check2 = st.columns([3, 1])
    with col_check1:
        check_login_input = st.text_input("Verificar disponibilidade de login", key="check_login_input")
    with col_check2:
        if st.button("Verificar", key="check_login_button"):
            if check_login_input:
                if check_login_input in usuarios:
                    st.error("❌ Login existente, digite novamente!")
                    if 'novo_login' in st.session_state:
                        del st.session_state['novo_login']
                else:
                    st.success("✅ Login disponível!")
                    st.session_state['novo_login'] = check_login_input
            else:
                st.warning("Digite um login primeiro.")
                if 'novo_login' in st.session_state:
                    del st.session_state['novo_login']

    with st.form("novo_usuario_form"):
        col1, col2 = st.columns(2)
        with col1:
            novo_login = st.text_input("Login do usuário*", value=st.session_state.get('novo_login', ''), key="novo_login_input")
            novo_nome = st.text_input("Nome completo*")
        with col2:
            nova_senha = st.text_input("Senha*", type="password")
            # Obter lista de lojas do banco de dados
            lojas_disponiveis = obter_lojas()
            lojas_disponiveis.insert(0, "Todas")
            nova_loja = st.selectbox("Loja de atuação*", options=lojas_disponiveis)
        
        st.markdown("**Permissões:**")
        col3, col4 = st.columns(2)
        with col3:
            novo_role = st.selectbox("Perfil*", ["user", "admin", "manager"])
            novo_ver_filtros = st.checkbox("Ver Filtros", value=False)
            novo_ver_indicadores = st.checkbox("Ver Indicadores", value=True)
            novo_ver_graficos = st.checkbox("Ver Gráficos", value=True)
        with col4:
            novo_executar_pipeline = st.checkbox("Executar Pipeline", value=False)
            novo_analisar_todas_lojas = st.checkbox("Analisar Todas as Lojas", value=False)
            novo_upload_csv = st.checkbox("Upload CSV", value=False)
        
        adicionar_usuario = st.form_submit_button("➕ Adicionar Usuário")

    if adicionar_usuario:
        if not novo_login or not novo_nome or not nova_senha or not nova_loja:
            st.error("❌ Preencha todos os campos obrigatórios (*)")
        elif novo_login in usuarios:
            st.error("❌ Login existente! Favor criar diferenciado.")
        else:
            try:
                # Definir permissões padrão baseadas no perfil
                if novo_role == "admin":
                    permissions_padrao = {
                        "ver_filtros": True,
                        "ver_indicadores": True,
                        "ver_graficos": True,
                        "executar_pipeline": True,
                        "analisar_todas_lojas": True,
                        "upload_csv": True
                    }
                elif novo_role == "manager":
                    permissions_padrao = {
                        "ver_filtros": True,
                        "ver_indicadores": True,
                        "ver_graficos": True,
                        "executar_pipeline": False,
                        "analisar_todas_lojas": False,  # Manager vê apenas sua loja
                        "upload_csv": False
                    }
                else:  # user
                    permissions_padrao = {
                        "ver_filtros": True,
                        "ver_indicadores": True,
                        "ver_graficos": True,
                        "executar_pipeline": False,
                        "analisar_todas_lojas": False,
                        "upload_csv": False
                    }

                novo_usuario = {
                    "password": nova_senha,
                    "role": novo_role,
                    "nome": novo_nome,
                    "loja": nova_loja,
                    "permissions": permissions_padrao
                }
                
                # Salvar no banco de dados
                salvar_usuario(
                    login=novo_login,
                    password=nova_senha,
                    role=novo_role,
                    nome=novo_nome,
                    loja=nova_loja,
                    permissions=novo_usuario["permissions"]
                )
                
                # Atualizar lista local
                usuarios[novo_login] = novo_usuario
                st.success(f"✅ Usuário '{novo_login}' adicionado com sucesso!")
                
            except Exception as e:
                st.error(f"❌ Erro ao adicionar usuário: {e}")
    
    st.markdown("---")
    st.subheader("Usuários Existentes")

    # 🔹 AGRUPAR USUÁRIOS POR LOJA E PERFIL
    grupos = {}
    for user, data in usuarios.items():
        if data.get("role") == "admin":
            grupo = "✅ Administradores"
        else:
            loja = data.get("loja", "Sem Loja")
            grupo = f"✅ {loja}" if loja != "Todas" else "Usuários Gerais"

        if grupo not in grupos:
            grupos[grupo] = []
        grupos[grupo].append((user, data))

    # 🔹 ORDENAR GRUPOS: ADMINISTRADORES PRIMEIRO, DEPOIS OS OUTROS ALFABETICAMENTE
    ordem_grupos = ["Administradores"] + sorted([g for g in grupos.keys() if g != "Administradores"])

    # 🔹 EXIBIR GRUPOS ORDENADOS
    for idx, grupo in enumerate(ordem_grupos):
        if grupo in grupos:
            # Adicionar separador discreto antes de cada grupo, exceto o primeiro
            if idx > 0:
                st.markdown("---")
            st.markdown(f"### {grupo}")
            # Ordenar usuários dentro do grupo por login alfabeticamente
            usuarios_grupo = sorted(grupos[grupo], key=lambda x: x[0].lower())

            for i, (user, data) in enumerate(usuarios_grupo):
                status_icon = "" if data.get("ativo", True) else ""
                status_text = "ativo" if data.get("ativo", True) else "desativado"
                st.subheader(f"{status_icon} {user} ({status_text})")

                col_info, col_perm = st.columns([1, 2])

                with col_info:
                    # Campos editáveis para informações do usuário
                    nome_editado = st.text_input(f"Nome", value=data.get("nome", ""), key=f"nome_{user}")

                    senha_editada = st.text_input(f"Senha", value=data.get("password", ""), type="password", key=f"senha_{user}")

                    # Selectbox para lojas
                    lojas_disponiveis = obter_lojas()
                    lojas_disponiveis.insert(0, "Todas")
                    loja_atual = data.get("loja", "Todas")
                    loja_editada = st.selectbox(
                        f"Loja",
                        options=lojas_disponiveis,
                        index=lojas_disponiveis.index(loja_atual) if loja_atual in lojas_disponiveis else 0,
                        key=f"loja_{user}"
                    )

                    role_atual = data.get("role", "user")
                    roles_options = ["admin", "manager", "user"]
                    role_index = roles_options.index(role_atual) if role_atual in roles_options else 2
                    role_editado = st.selectbox(
                        f"Perfil",
                        roles_options,
                        index=role_index,
                        key=f"role_{user}_config"
                    )

                with col_perm:
                    st.markdown("**Permissões:**")
                    perms = data.get("permissions", {})
                    role_atual = data.get("role", "user")

                    # Para manager, mostrar permissões padrão e não editáveis
                    if role_atual == "manager":
                        st.info("Perfil Manager: Permissões padrão aplicadas automaticamente")
                        novo_ver_filtros = True
                        novo_ver_indicadores = True
                        novo_ver_graficos = True
                        novo_executar_pipeline = False
                        novo_analisar_todas_lojas = False
                        novo_upload_csv = False
                    else:
                        # Para admin e user, permitir edição
                        novo_ver_filtros = st.checkbox("Ver Filtros", value=perms.get("ver_filtros", False), key=f"filtros_{user}_config")
                        novo_ver_indicadores = st.checkbox("Ver Indicadores", value=perms.get("ver_indicadores", True), key=f"ind_{user}_config")
                        novo_ver_graficos = st.checkbox("Ver Gráficos", value=perms.get("ver_graficos", True), key=f"graf_{user}_config")
                        novo_executar_pipeline = st.checkbox("Executar Pipeline", value=perms.get("executar_pipeline", False), key=f"pipe_{user}_config")
                        novo_analisar_todas_lojas = st.checkbox("Analisar Todas as Lojas", value=perms.get("analisar_todas_lojas", False), key=f"lojas_{user}_config")
                        novo_upload_csv = st.checkbox("Upload CSV", value=perms.get("upload_csv", False), key=f"upload_{user}_config")

                # Botão para atualizar usuário
                if st.button(f"Atualizar {user}", key=f"update_{user}"):
                    try:
                        updated_permissions = {
                            "ver_filtros": novo_ver_filtros,
                            "ver_indicadores": novo_ver_indicadores,
                            "ver_graficos": novo_ver_graficos,
                            "executar_pipeline": novo_executar_pipeline,
                            "analisar_todas_lojas": novo_analisar_todas_lojas,
                            "upload_csv": novo_upload_csv
                        }

                        salvar_usuario(
                            login=user,
                            password=senha_editada,
                            role=role_editado,
                            nome=nome_editado,
                            loja=loja_editada,
                            permissions=updated_permissions
                        )

                        # Atualizar lista local
                        usuarios[user] = {
                            "password": senha_editada,
                            "role": role_editado,
                            "nome": nome_editado,
                            "loja": loja_editada,
                            "permissions": updated_permissions
                        }

                        st.success(f"✅ Usuário {user} atualizado com sucesso!")

                    except Exception as e:
                        st.error(f"❌ Erro ao atualizar usuário: {e}")

                # Botão para desativar/reativar usuário (somente admin)
                if st.session_state.role == "admin":
                    usuario_atual = usuarios[user]
                    status_atual = usuario_atual.get("ativo", True)

                    if status_atual:
                        # Usuário ativo - mostrar botão para desativar
                        if st.button(f"Desativar {user}", key=f"deactivate_{user}"):
                            try:
                                # Atualizar status no banco
                                salvar_usuario(
                                    login=user,
                                    password=usuario_atual["password"],
                                    role=usuario_atual["role"],
                                    nome=usuario_atual["nome"],
                                    loja=usuario_atual["loja"],
                                    permissions=usuario_atual["permissions"],
                                    ativo=False
                                )

                                # Atualizar lista local
                                usuarios[user]["ativo"] = False
                                st.success(f"✅ Usuário {user} desativado com sucesso!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erro ao desativar usuário: {e}")
                    else:
                        # Usuário desativado - mostrar botão para reativar
                        if st.button(f"Reativar {user}", key=f"reactivate_{user}"):
                            try:
                                # Atualizar status no banco
                                salvar_usuario(
                                    login=user,
                                    password=usuario_atual["password"],
                                    role=usuario_atual["role"],
                                    nome=usuario_atual["nome"],
                                    loja=usuario_atual["loja"],
                                    permissions=usuario_atual["permissions"],
                                    ativo=True
                                )

                                # Atualizar lista local
                                usuarios[user]["ativo"] = True
                                st.success(f"✅ Usuário {user} reativado com sucesso!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erro ao reativar usuário: {e}")

                # 🔹 PULAR LINHA entre usuários dentro do grupo (exceto após o último do grupo)
                if i < len(usuarios_grupo) - 1:
                    st.markdown("<br>", unsafe_allow_html=True)

            # 🔹 PULAR LINHA entre grupos (exceto após o último grupo)
            if grupo != ordem_grupos[-1]:
                st.markdown("<br>", unsafe_allow_html=True)
    
    # 🔹 BOTÕES NA SIDEBAR (no lugar da engrenagem)
    with st.sidebar:
        st.markdown("---")
                
        if st.button("Recarregar Usuários", use_container_width=True, key="reload_users"):
            try:
                usuarios = carregar_usuarios()
                st.success("✅ Usuários recarregados do banco com sucesso!")
            except Exception as e:
                st.error(f"❌ Erro ao recarregar usuários: {e}")
        
        if st.button("Voltar ao Dashboard", use_container_width=True, key="voltar_dashboard"):
            st.session_state.show_config = False
            st.rerun()

# 🔹 Dashboard Principal (quando não está no modo configuração)
else:
    # 🔹 Upload ou carregamento do banco
    with st.expander("Importar Dados", expanded=False):
        if st.session_state.permissions.get("upload_csv", True):
            uploaded_file = st.file_uploader("Envie seu arquivo CSV", type="csv")
        else:
            uploaded_file = None

    if uploaded_file:
        try:
            # Detectar separador automaticamente
            separador = detectar_separador(uploaded_file)
            st.info(f"Separador detectado por {'vírgula' if separador == ',' else 'ponto e vírgula'}")

            # Ler CSV com separador detectado
            df = pd.read_csv(uploaded_file, sep=separador)

            # Validar e padronizar estrutura
            df = validar_e_padronizar_csv(df)

            st.success("✅ CSV carregado com sucesso!")

            with st.expander("Pré-visualização dos dados", expanded=False):
                st.dataframe(df.head(10))

                # Mostrar informações sobre o arquivo
                col_info1, col_info2, col_info3 = st.columns(3)
                with col_info1:
                    st.metric("Linhas", len(df))
                with col_info2:
                    st.metric("Colunas", len(df.columns))
                with col_info3:
                    st.metric("Colunas obrigatórias", len([col for col in ['nome_cliente', 'nome_produto', 'quantidade', 'valor_produto', 'nome_loja', 'nome_vendedor', 'data_venda'] if col in df.columns]))

        except ValueError as e:
            st.error(f"❌ Erro na validação do CSV: {e}")
            st.warning("Verifique se o arquivo CSV contém as colunas obrigatórias: nome_cliente, nome_produto, quantidade, valor_produto, nome_loja, nome_vendedor, data_venda")
            df = pd.DataFrame()  # DataFrame vazio para evitar erros downstream
        except Exception as e:
            st.error(f"❌ Erro ao processar o arquivo CSV: {e}")
            st.info("Certifique-se de que o arquivo é um CSV válido e tente novamente.")
            df = pd.DataFrame()  # DataFrame vazio para evitar erros downstream
    else:
        #st.subheader("Carregando dados")
        df = carregar_dados_sqlite()

        # Se não há dados no banco, tentar carregar automaticamente do CSV
        if df.empty:
            csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "vendas_clean.csv")
            if os.path.exists(csv_path):
                st.info("🔄 Carregando dados automaticamente do arquivo CSV...")
                try:
                    # Carregar e processar CSV automaticamente
                    df_csv = pd.read_csv(csv_path, sep=";", dtype=str)

                    # Validar e padronizar estrutura
                    df_csv = validar_e_padronizar_csv(df_csv)

                    # Processar dados (correção e validação)
                    linhas_corrigidas = []
                    for _, row in df_csv.iterrows():
                        row = corrigir_linha(row)
                        erros = validar_linha(row)
                        row_dict = row.to_dict()
                        row_dict['erros'] = ", ".join(erros) if erros else ""
                        linhas_corrigidas.append(row_dict)

                    df = pd.DataFrame(linhas_corrigidas)

                    # Limpeza e formatação
                    df["nome_cliente"] = df["nome_cliente"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else "")
                    df["bairro"] = df["bairro"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else "")
                    df["cidade"] = df["cidade"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else "")
                    df["forma_pagamento"] = df["forma_pagamento"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else "")
                    df["nome_vendedor"] = df["nome_vendedor"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else "")
                    df["endereco"] = df.apply(lambda x: str(x["endereco"]).split(",")[0] if pd.notna(x["endereco"]) else "", axis=1)
                    df["telefone"] = df["telefone"].astype(str).str.extract(r'(\d{10,11})')[0]

                    # Datas
                    for coluna in ["data_compra", "data_venda"]:
                        df[coluna + "_dt"] = pd.to_datetime(df[coluna], format="%d/%m/%Y", errors="coerce")
                        df[coluna] = df[coluna + "_dt"].dt.strftime("%d/%m/%Y")

                    def preencher_data_nascimento(valor):
                        try:
                            dt = pd.to_datetime(valor, dayfirst=True, errors='coerce')
                            if pd.isna(dt):
                                idade = random.randint(18, 65)
                                ano = pd.Timestamp.today().year - idade
                                mes = random.randint(1, 12)
                                dia = random.randint(1, 28)
                                dt = pd.Timestamp(year=ano, month=mes, day=dia)
                            return dt.strftime("%d/%m/%Y")
                        except:
                            return ""
                    df["data_nascimento"] = df["data_nascimento"].apply(preencher_data_nascimento)

                    st.success("✅ Dados carregados automaticamente do CSV!")

                except Exception as e:
                    st.error(f"❌ Erro ao carregar CSV automaticamente: {e}")
                    st.warning("⚠️ Nenhum dado encontrado. Use 'Importar Dados' para carregar um arquivo CSV.")
                    st.stop()
            else:
                st.warning("⚠️ Nenhum dado encontrado. Use 'Importar Dados' para carregar um arquivo CSV.")
                st.stop()

    # 🔹 Executar Pipeline (sempre disponível após carregamento dos dados)
    with st.expander("Executar Pipeline", expanded=False):
        if st.session_state.permissions.get("executar_pipeline", True):
            enviar_dropbox = st.checkbox("☁️ Enviar relatório para Dropbox")
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button("▶️ Executar pipeline"):
                    nova_linha()
                    result = executar_pipeline(df, enviar_dropbox)
                    st.success("✅ Pipeline executado com sucesso!")

                    relatorios_pdf = sorted(
                        [f for f in os.listdir(REPORTS_DIR) if f.startswith("relatorio_qualidade_") and f.endswith(".pdf")],
                        reverse=True
                    )
                    if relatorios_pdf:
                        pdf_path = os.path.join(REPORTS_DIR, relatorios_pdf[0])
                        st.markdown(f"[⬇️ Baixar relatório PDF]({pdf_path})")
                    else:
                        st.info("Nenhum relatório PDF encontrado.")

                    if enviar_dropbox and result.get("link_publico"):
                        st.markdown(f"📎 [Abrir relatório no Dropbox]({result['link_publico']})")
            with col2:
                pass  # Checkbox moved above

    # 🔹 Validação e correção (otimizada para memória)
    linhas_corrigidas = []
    chunk_size = 1000

    # Processar em chunks para evitar carregar tudo na memória
    for start_idx in range(0, len(df), chunk_size):
        end_idx = min(start_idx + chunk_size, len(df))
        chunk_df = df.iloc[start_idx:end_idx]

        for _, row in chunk_df.iterrows():
            row = corrigir_linha(row)
            erros = validar_linha(row)
            row_dict = row.to_dict()
            row_dict['erros'] = ", ".join(erros) if erros else ""
            linhas_corrigidas.append(row_dict)

        # Limpar chunk da memória
        del chunk_df

    df_corrigido = pd.DataFrame(linhas_corrigidas)
    # Limpar lista após criar DataFrame
    del linhas_corrigidas

    # 🔹 Limpeza e formatação
    def formatar_texto(texto):
        if pd.isna(texto):
            return ""
        s = str(texto)
        # Remove títulos
        for prefixo in ["Sr.", "Sra.", "Dr.", "Dra.", "Srta."]:
            s = s.replace(prefixo, "")
        s = s.strip()
        return unidecode.unidecode(s)

    df_corrigido["nome_cliente"] = df_corrigido["nome_cliente"].apply(formatar_texto)
    df_corrigido["bairro"] = df_corrigido["bairro"].apply(formatar_texto)
    df_corrigido["cidade"] = df_corrigido["cidade"].apply(formatar_texto)
    df_corrigido["forma_pagamento"] = df_corrigido["forma_pagamento"].apply(formatar_texto)
    df_corrigido["nome_vendedor"] = df_corrigido["nome_vendedor"].apply(formatar_texto)

    # Corrigir endereço
    df_corrigido["endereco"] = df_corrigido.apply(lambda x: str(x["endereco"]).split(",")[0] if pd.notna(x["endereco"]) else "", axis=1)

    # Telefone: manter apenas a partir do DDD
    df_corrigido["telefone"] = df_corrigido["telefone"].astype(str).str.extract(r'(\d{10,11})')[0]

    # Datas: criar colunas datetime
    for coluna in ["data_compra", "data_venda"]:
        # Assume formato dd/mm/yyyy gerado pelo populate.py
        df_corrigido[coluna + "_dt"] = pd.to_datetime(df_corrigido[coluna], format="%d/%m/%Y", errors="coerce")
        df_corrigido[coluna] = df_corrigido[coluna + "_dt"].dt.strftime("%d/%m/%Y")

    # Preencher data_nascimento
    def preencher_data_nascimento(valor):
        try:
            dt = pd.to_datetime(valor, dayfirst=True, errors='coerce')
            if pd.isna(dt):
                idade = random.randint(18, 65)
                ano = pd.Timestamp.today().year - idade
                mes = random.randint(1, 12)
                dia = random.randint(1, 28)
                dt = pd.Timestamp(year=ano, month=mes, day=dia)
            return dt.strftime("%d/%m/%Y")
        except:
            return ""
    df_corrigido["data_nascimento"] = df_corrigido["data_nascimento"].apply(preencher_data_nascimento)

    # 🔹 Filtros na sidebar
    st.sidebar.header("Filtros")

    # Aplicar permissões para filtros
    if st.session_state.permissions.get("ver_filtros", True):
        # Para user: filtrar apenas pela loja e vendedor atribuído (apenas seus próprios dados)
        if st.session_state.role == "user":
            # User vê apenas dados da sua loja e apenas suas próprias vendas
            filtro_loja = [st.session_state.loja_usuario]
            filtro_vendedor = [st.session_state.nome_usuario]

        # Para manager, filtrar apenas pela loja de atuação
        elif st.session_state.role == "manager" and st.session_state.loja_usuario != "Todas":
            # Manager vê apenas dados da sua loja
            filtro_loja = [st.session_state.loja_usuario]
            filtro_vendedor = st.sidebar.multiselect("Vendedor",
                df_corrigido[df_corrigido["nome_loja"] == st.session_state.loja_usuario]["nome_vendedor"].dropna().unique(),
                default=list(df_corrigido[df_corrigido["nome_loja"] == st.session_state.loja_usuario]["nome_vendedor"].dropna().unique()))
        else:
            # Admin ou manager com "Todas" as lojas
            filtro_loja = st.sidebar.multiselect("Loja", df_corrigido["nome_loja"].dropna().unique(),
                                                 default=list(df_corrigido["nome_loja"].dropna().unique()))
            filtro_vendedor = st.sidebar.multiselect("Vendedor", df_corrigido["nome_vendedor"].dropna().unique(),
                                                     default=list(df_corrigido["nome_vendedor"].dropna().unique()))

        # Filtros adicionais para admin/manager
        if st.session_state.role in ["admin", "manager"]:
            filtro_pagamento = st.sidebar.multiselect("Forma de Pagamento", df_corrigido["forma_pagamento"].dropna().unique(),
                                                      default=list(df_corrigido["forma_pagamento"].dropna().unique()))
            filtro_produto = st.sidebar.multiselect("Produto", df_corrigido["nome_produto"].dropna().unique(),
                                                   default=list(df_corrigido["nome_produto"].dropna().unique()))

            filtro_status_usuario = st.sidebar.multiselect(
                "Status do Usuário",
                ["Ativo", "Desativado"],
                default=["Ativo"],  # Por padrão, mostrar apenas ativos
                help="Filtrar vendas por status do usuário responsável"
            )
        else:
            # Para user: filtros limitados ou nenhum
            filtro_pagamento = df_corrigido["forma_pagamento"].dropna().unique()
            filtro_produto = df_corrigido["nome_produto"].dropna().unique()
            filtro_status_usuario = ["Ativo"]  # Usuários normais só veem ativos
    else:
        # Se não tem permissão para ver filtros, usar todos os valores
        filtro_loja = df_corrigido["nome_loja"].dropna().unique()
        filtro_vendedor = df_corrigido["nome_vendedor"].dropna().unique()
        filtro_pagamento = df_corrigido["forma_pagamento"].dropna().unique()
        filtro_produto = df_corrigido["nome_produto"].dropna().unique()
        filtro_status_usuario = ["Ativo"]

    # 🔹 Filtro de datas (data_venda) - formato brasileiro
    data_min = df_corrigido["data_venda_dt"].min().date()
    data_max = df_corrigido["data_venda_dt"].max().date()
    inicio = st.sidebar.date_input("Data inicial - Venda (dd/mm/aaaa)", value=data_min, format="DD/MM/YYYY")
    fim = st.sidebar.date_input("Data final - Venda (dd/mm/aaaa)", value=data_max, format="DD/MM/YYYY")
    st.sidebar.caption("Calendário e datas no padrão brasileiro: dia/mês/ano. Se o calendário aparecer em inglês, ajuste o idioma do navegador para português.")

    # 🔹 Aplicar filtros
    if st.session_state.permissions.get("analisar_todas_lojas", False):
        # Se pode analisar todas as lojas, não filtra por loja
        df_filtrado = df_corrigido[
            (df_corrigido["nome_vendedor"].isin(filtro_vendedor)) &
            (df_corrigido["forma_pagamento"].isin(filtro_pagamento)) &
            (df_corrigido["nome_produto"].isin(filtro_produto)) &
            (df_corrigido["data_venda_dt"] >= pd.to_datetime(inicio)) &
            (df_corrigido["data_venda_dt"] <= pd.to_datetime(fim))
        ]

        # Dividir 'Cartão' em 'Cartão de Débito' e 'Cartão de Crédito'
        cartao_rows = df_filtrado[df_filtrado['forma_pagamento'] == 'Cartão']
        if not cartao_rows.empty:
            num_cartao = len(cartao_rows)
            num_debito = num_cartao // 2
            debito_rows = cartao_rows.iloc[:num_debito].copy()
            debito_rows['forma_pagamento'] = 'Cartão de Débito'
            credito_rows = cartao_rows.iloc[num_debito:].copy()
            credito_rows['forma_pagamento'] = 'Cartão de Crédito'
            df_filtrado = df_filtrado[df_filtrado['forma_pagamento'] != 'Cartão']
            df_filtrado = pd.concat([df_filtrado, debito_rows, credito_rows], ignore_index=True)
    else:
        # Aplicar filtros baseados no perfil do usuário
        df_filtrado = df_corrigido[
            (df_corrigido["nome_loja"].isin(filtro_loja)) &
            (df_corrigido["nome_vendedor"].isin(filtro_vendedor)) &
            (df_corrigido["forma_pagamento"].isin(filtro_pagamento)) &
            (df_corrigido["nome_produto"].isin(filtro_produto)) &
            (df_corrigido["data_venda_dt"] >= pd.to_datetime(inicio)) &
            (df_corrigido["data_venda_dt"] <= pd.to_datetime(fim))
        ]

        # Dividir 'Cartão' em 'Cartão de Débito' e 'Cartão de Crédito'
        cartao_rows = df_filtrado[df_filtrado['forma_pagamento'] == 'Cartão']
        if not cartao_rows.empty:
            num_cartao = len(cartao_rows)
            num_debito = num_cartao // 2
            debito_rows = cartao_rows.iloc[:num_debito].copy()
            debito_rows['forma_pagamento'] = 'Cartão de Débito'
            credito_rows = cartao_rows.iloc[num_debito:].copy()
            credito_rows['forma_pagamento'] = 'Cartão de Crédito'
            df_filtrado = df_filtrado[df_filtrado['forma_pagamento'] != 'Cartão']
            df_filtrado = pd.concat([df_filtrado, debito_rows, credito_rows], ignore_index=True)

    # 🔹 Aplicar filtro de status do usuário (se aplicável)
    if 'filtro_status_usuario' in locals() and filtro_status_usuario:
        # Para aplicar filtro de status, precisamos mapear vendas para usuários
        # Por enquanto, manter todas as vendas visíveis para admin/manager
        # Futuramente pode ser implementado mapeamento venda->usuário
        pass



    # 🔹 VERIFICAR SE df_filtrado EXISTE ANTES DE USAR
    if 'df_filtrado' not in locals() and 'df_filtrado' not in globals():
        # Se df_filtrado não foi definido, usar df_corrigido como fallback
        df_filtrado = df_corrigido.copy()

    # 🔹 Indicadores de Vendas
    if st.session_state.permissions.get("ver_indicadores", True) and not df_filtrado.empty:
        try:
            # Calcular valor total considerando quantidade
            df_filtrado['valor_total_calculado'] = df_filtrado['valor_produto'] * df_filtrado['quantidade']
            valor_total = df_filtrado['valor_total_calculado'].sum()
            total_vendas = len(df_filtrado)
            ticket_medio = valor_total / total_vendas if total_vendas > 0 else 0
            
            # CORREÇÃO: Garantir que estamos pegando valores numéricos
            vendas_por_loja = df_filtrado.groupby("nome_loja")['valor_total_calculado'].sum().sort_values(ascending=False)
            vendas_por_produto = df_filtrado.groupby("nome_produto")['valor_total_calculado'].sum().sort_values(ascending=False)
            vendas_por_vendedor = df_filtrado.groupby("nome_vendedor")['valor_total_calculado'].sum().sort_values(ascending=False)

            nova_linha()
            st.markdown("### Indicadores de Vendas")
            
            # Aplicar estilos CSS
            st.markdown("""
            <style>
            .stMetricValue {
                font-size: 10px !important;
            }
            .stMetricDelta {
                font-size: 10px !important;
            }
            </style>
            """, unsafe_allow_html=True)
            
            card1, card2, card3, card4, card5 = st.columns(5)
            
            with card1:
                st.metric("Valor Total", f"R$ {valor_total:,.2f}", f"{total_vendas} transações")
            
            with card2:
                if not vendas_por_loja.empty:
                    loja_top = vendas_por_loja.index[0]
                    valor_loja = vendas_por_loja.iloc[0]
                    st.metric("Loja Destaque", loja_top, delta=f"R$ {valor_loja:,.2f}")
            
            with card3:
                if not vendas_por_produto.empty:
                    produto_top = vendas_por_produto.index[0]
                    valor_produto = vendas_por_produto.iloc[0]
                    st.metric("Campeão de Vendas", produto_top, delta=f"R$ {valor_produto:,.2f}")
            
            with card4:
                if not vendas_por_vendedor.empty:
                    vendedor_top = vendas_por_vendedor.index[0]
                    valor_vendedor = vendas_por_vendedor.iloc[0]
                    st.metric("Vendedor Destaque", vendedor_top, delta=f"R$ {valor_vendedor:,.2f}")
            
            with card5:
                st.metric("Média por Transação", f"R$ {ticket_medio:,.2f}")

            nova_linha()
            
        except Exception as e:
            st.error(f"❌ Erro ao calcular indicadores: {e}")

    # 🔹 Gráficos Interativos
    if st.session_state.permissions.get("ver_graficos", True) and not df_filtrado.empty:
        st.subheader("Insights Interativos")
        
        # 🔹 PALETAS DE CORES DEFINIDAS
        loja_colors = ['#9B59B6', "#B8860B", "#2B50E4"]  # Lilac, Aged Gold, Blue for nome_loja
        pie_colors = ['#9B59B6', '#B8860B', '#2B50E4', "#989795", '#27AE60']  # Lilac, Aged Gold, Blue, Orange, Green for pie
        product_colors = ['#9B59B6', '#B8860B', '#2B50E4']  # Same for products
        cores_lilas = ['#9B59B6', '#B8860B', '#2B50E4']  # For bars and lines

        # 🔹 MAPA DE CORES FIXO PARA LOJAS (consistente entre perfis)
        loja_color_map = {
            'Loja Bairro': "#E68422",
            'Loja Centro': '#9B59B6',
            'Loja Shopping': '#2B50E4'
        }

        # 🔹 MAPA PARA NOMES DE EXIBIÇÃO DAS LOJAS
        loja_display_map = {
            'Loja Bairro': 'Bairro',
            'Loja Centro': 'Centro',
            'Loja Shopping': 'Shopping'
        }
        
        tab1, tab2, tab3 = st.tabs(["Por Loja", "Evolução Temporal", "Por Produto"])
        
        with tab1:
            col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            # Vendas por loja
            vendas_loja = df_filtrado.groupby('nome_loja').agg({
                'valor_total_calculado': 'sum'
            }).reset_index()

            # Criar coluna com nome curto da loja (remover "Loja ")
            vendas_loja['nome_loja_curto'] = vendas_loja['nome_loja'].str.replace('Loja ', '', regex=False)

            # 🔹 FORMATAR VALOR PARA BRASILEIRO NO HOVER
            vendas_loja['valor_formatado'] = vendas_loja['valor_total_calculado'].apply(
                lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
            )

            # 🔹 MAPEAR CORES PARA LOJAS NO DATAFRAME
            vendas_loja['cor_loja'] = vendas_loja['nome_loja_curto'].map(loja_color_map)

            # 🔹 CRIAR COLUNA PARA EXIBIÇÃO SIMPLIFICADA
            vendas_loja['nome_loja_display'] = vendas_loja['nome_loja'].map(loja_display_map)

            fig_lojas = px.bar(
                vendas_loja,
                x='nome_loja_display',
                y='valor_total_calculado',
                title="Vendas por Loja",
                color='nome_loja',
                color_discrete_map=loja_color_map,  # 🔹 USAR MAPA FIXO DE CORES
                labels={'nome_loja_display': 'Loja', 'valor_total_calculado': 'Valor Total (R$)'},
                text=vendas_loja['valor_formatado']  # 🔹 TEXTO FORMATADO PARA HOVER
            )

            # 🔹 FORMATAR EIXO Y PARA MOSTRAR R$ EM VEZ DE K
            fig_lojas.update_layout(
                title={
                    'text': "Vendas por loja",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {
                        'size': 18,
                        'family': "Arial, sans-serif"
                    }
                },
                yaxis=dict(
                    tickformat='R$ ,.2f'  # 🔹 FORMATAÇÃO EM REAIS COM 2 CASAS DECIMAIS
                ),
                legend_title_text=None,  # 🔹 REMOVER TÍTULO DA LEGENDA
                legend=dict(font=dict(size=14))  # 🔹 AUMENTAR FONTE DA LEGENDA
            )

            # 🔹 FORMATAR HOVER PARA BRASILEIRO
            fig_lojas.update_traces(
                hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
                textposition='none'  # 🔹 NÃO MOSTRAR TEXTO NAS BARRAS
            )

            st.plotly_chart(fig_lojas, use_container_width=True)

            with col_chart2:
                # MELHORIA: Top 10 vendedores mostrando a LOJA
                top_vendedores_com_loja = df_filtrado.groupby(['nome_vendedor', 'nome_loja']).agg({
                    'valor_total_calculado': 'sum'
                }).reset_index().nlargest(10, 'valor_total_calculado')

                # Criar coluna com nome curto da loja (remover "Loja ")
                top_vendedores_com_loja['nome_loja_curto'] = top_vendedores_com_loja['nome_loja'].str.replace('Loja ', '', regex=False)

                # 🔹 CRIAR COLUNA PARA EXIBIÇÃO SIMPLIFICADA
                top_vendedores_com_loja['nome_loja_display'] = top_vendedores_com_loja['nome_loja'].map(loja_display_map)

                # 🔹 FORMATAR VALOR PARA BRASILEIRO NO HOVER
                top_vendedores_com_loja['valor_formatado'] = top_vendedores_com_loja['valor_total_calculado'].apply(
                    lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
                )

                # Ordenar por valor total (maior para menor) para manter cores consistentes
                top_vendedores_com_loja = top_vendedores_com_loja.sort_values('valor_total_calculado', ascending=True)

                fig_vendedores = px.bar(
                    top_vendedores_com_loja,
                    x='valor_total_calculado',
                    y='nome_vendedor',
                    orientation='h',
                    title="Total por vendedor",
                    color='nome_loja',
                    color_discrete_map=loja_color_map,  # 🔹 USAR MAPA FIXO DE CORES
                    labels={'valor_total_calculado': 'Valor Total (R$)', 'nome_vendedor': 'Vendedor', 'nome_loja_display': 'Loja'},
                    text=top_vendedores_com_loja['valor_formatado']  # 🔹 TEXTO FORMATADO PARA HOVER
                )

                # 🔹 FORMATAR EIXO X PARA MOSTRAR R$ EM VEZ DE K
                fig_vendedores.update_layout(
                    yaxis={'categoryorder': 'total ascending'},
                    title={
                        'text': "Total por vendedor",
                        'x': 0.5,
                        'xanchor': 'center',
                        'font': {
                            'size': 18,
                            'family': "Arial, sans-serif"
                        }
                    },
                    xaxis=dict(
                        tickformat='R$ ,.2f'  # 🔹 FORMATAÇÃO EM REAIS COM 2 CASAS DECIMAIS
                    ),
                    legend_title_text=None,  # 🔹 REMOVER TÍTULO DA LEGENDA
                    legend=dict(font=dict(size=14))  # 🔹 AUMENTAR FONTE DA LEGENDA
                )

                # 🔹 FORMATAR HOVER PARA BRASILEIRO
                fig_vendedores.update_traces(
                    hovertemplate='<b>%{y}</b><br>%{text}<br>Loja: %{fullData.name}<extra></extra>',
                    textposition='none'  # 🔹 NÃO MOSTRAR TEXTO NAS BARRAS
                )

                st.plotly_chart(fig_vendedores, use_container_width=True)

    with tab2:
        col_chart3, col_chart4 = st.columns(2)

        with col_chart3:
                # MELHORIA: Evolução temporal por LOJA com legenda interativa
                if 'data_venda_dt' in df_filtrado.columns:
                    # Criar colunas de mês e ano para agrupamento mensal
                    df_filtrado_copy = df_filtrado.copy()
                    df_filtrado_copy['month'] = df_filtrado_copy['data_venda_dt'].dt.month
                    df_filtrado_copy['year'] = df_filtrado_copy['data_venda_dt'].dt.year

                    # Agrupar por mês, ano e loja (agregação mensal)
                    evolucao_lojas = df_filtrado_copy.groupby(['year', 'month', 'nome_loja']).agg({
                        'valor_total_calculado': 'sum'
                    }).reset_index()

                    # Ordenar cronologicamente por ano e mês, e dentro de cada período por valor ascendente para empilhar do menor para o maior
                    evolucao_lojas = evolucao_lojas.sort_values(['year', 'month', 'valor_total_calculado'])

                    # Criar coluna com nome curto da loja (remover "Loja ")
                    evolucao_lojas['nome_loja_curto'] = evolucao_lojas['nome_loja'].str.replace('Loja ', '', regex=False)

                    # 🔹 CRIAR COLUNA PARA EXIBIÇÃO SIMPLIFICADA
                    evolucao_lojas['nome_loja_display'] = evolucao_lojas['nome_loja'].map(loja_display_map)

                    # Criar coluna de data formatada em português brasileiro
                    meses_pt = {
                        1: 'jan', 2: 'fev', 3: 'mar', 4: 'abr', 5: 'mai', 6: 'jun',
                        7: 'jul', 8: 'ago', 9: 'set', 10: 'out', 11: 'nov', 12: 'dez'
                    }
                    evolucao_lojas['data_display'] = evolucao_lojas.apply(
                        lambda row: f"{meses_pt[row['month']]} {row['year']}", axis=1
                    )

                    fig_evolucao = px.bar(
                        evolucao_lojas,
                        x='data_display',
                        y='valor_total_calculado',
                        color='nome_loja',
                        title="Evolução das vendas por loja",
                        color_discrete_map=loja_color_map,  # 🔹 USAR MAPA FIXO DE CORES
                        labels={'data_display': 'Data', 'valor_total_calculado': 'Valor Total (R$)', 'nome_loja_display': 'Loja'}
                    )

                    # MELHORIA: Tornar o gráfico mais interativo com legenda clicável
                    fig_evolucao.update_layout(
                        hovermode='x unified',
                        showlegend=True,
                        barmode='stack',  # 🔹 EMPILHAR BARRAS PARA MOSTRAR TOTAL POR PERÍODO
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1,
                            # Adiciona interatividade à legenda
                            itemclick="toggle",
                            itemdoubleclick="toggleothers",
                            font=dict(size=14)  # 🔹 AUMENTAR FONTE DA LEGENDA
                        ),
                        yaxis=dict(
                            tickformat='R$ ,.2f'  # 🔹 FORMATAÇÃO EM REAIS COM 2 CASAS DECIMAIS
                        ),
                        xaxis=dict(
                            categoryorder='array',
                            categoryarray=evolucao_lojas['data_display'].unique()
                        ),
                        legend_title_text=None  # 🔹 REMOVER TÍTULO DA LEGENDA
                    )

                    # MELHORIA: Adicionar funcionalidades de interação
                    fig_evolucao.update_traces(
                        # Permite mostrar/esconder linhas clicando na legenda
                        legendgrouptitle_font_size=12,
                        # Adiciona mais interatividade
                        hovertemplate='<b>%{fullData.name}</b><br>Data: %{x}<br>Valor: R$ %{y:,.2f}<extra></extra>'
                    )

                    # 🔹 Remover valores totais da legenda, manter apenas nomes das lojas
                    # A legenda agora mostra apenas os nomes das lojas

                    # 🔹 Reverter a ordem da legenda para mostrar totais de maior para menor
                    fig_evolucao.update_layout(legend_traceorder='reversed')

                    # 🔹 Remover anotações com valores dentro do gráfico
                    # Os valores mensais não serão exibidos no gráfico

                    st.plotly_chart(fig_evolucao, use_container_width=True)

        with col_chart4:
            # Formas de pagamento - CORREÇÃO: Mostrar valor e porcentagem FORA DO GRÁFICO
            pagamentos = df_filtrado.groupby('forma_pagamento').agg({
                'valor_total_calculado': 'sum',
                'quantidade': 'count'
            }).reset_index()

            # Calcular porcentagem
            total_valor = pagamentos['valor_total_calculado'].sum()
            pagamentos['porcentagem'] = (pagamentos['valor_total_calculado'] / total_valor * 100).round(1)

            # Formatar valores para exibição
            pagamentos['valor_formatado'] = pagamentos['valor_total_calculado'].apply(
                lambda x: f'R$ {x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
            )

            fig_pagamentos = px.pie(
                pagamentos,
                names='forma_pagamento',
                values='valor_total_calculado',
                title="Forma De Pagamento",
                color_discrete_map={'Cartão de Débito': '#9B59B6', 'Cartão de Crédito': '#B8860B', 'Dinheiro': '#2B50E4', 'Pix': '#E67E22', 'Boleto': '#27AE60'},
                custom_data=['valor_formatado', 'porcentagem']
            )

            # 🔹 MODIFICAÇÃO: Texto fora do gráfico em negrito
            fig_pagamentos.update_traces(
                textposition='outside',  # Texto fora do gráfico
                texttemplate='<b>%{customdata[0]}</b><br><b>(%{customdata[1]}%)</b>',  # Texto em negrito
                hovertemplate='<b>%{label}</b><br>Valor: %{customdata[0]}<br>Percentual: %{customdata[1]:.1f}%',
                marker=dict(line=dict(color='#ffffff', width=2))
            )

            fig_pagamentos.update_layout(
                title={
                    'text': "Distribuição por orma de pagamento",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {
                        'size': 18,
                        'family': "Arial, sans-serif"
                    }
                },
                # Ajustar margens para acomodar texto externo
                margin=dict(t=80, b=80, l=80, r=80),
                showlegend=True,
                legend=dict(
                    orientation="v",
                    yanchor="middle",
                    y=0.7,
                    xanchor="left",
                    x=1.15,
                    font=dict(size=14)
                ),
                # Configurações para texto externo
                uniformtext_minsize=13.5,
                uniformtext_mode='hide'
            )

            st.plotly_chart(fig_pagamentos, use_container_width=True)
        
        with tab3:
            col_chart5, col_chart6 = st.columns(2)
            
            with col_chart5:
                # Produtos mais vendidos - AGORA EM CARDS
                produtos_vendidos = df_filtrado.groupby('nome_produto').agg({
                    'quantidade': 'sum'
                }).nlargest(10, 'quantidade').reset_index()

                nova_linha()

                # Calcular porcentagem para cada produto
                total_quantidade = produtos_vendidos['quantidade'].sum()
                produtos_vendidos['porcentagem'] = (produtos_vendidos['quantidade'] / total_quantidade * 100).round(1)

                st.markdown("""
                    <h3 style='font-size: 18px; font-weight: bold; margin-bottom: 20px; text-align: center;'>
                        Produtos mais vendidos (Quantidade)
                    </h3>
                """, unsafe_allow_html=True)

                cols = st.columns(3)
                
                for i, (_, produto) in enumerate(produtos_vendidos.head(6).iterrows()):
                    with cols[i % 3]:
                        st.markdown(
                            f"""
                            <div style='
                                background: #2B50E4;
                                padding: 15px;
                                border-radius: 10px;
                                margin: 8px 0;
                                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                                color: white;
                                text-align: center;
                                height: 120px;
                                display: flex;
                                flex-direction: column;
                                justify-content: center;
                            '>
                                <h4 style='margin: 0; font-size: 14px; font-weight: bold;'>#{i+1} {produto['nome_produto']}</h4>
                                <div style='font-size: 20px; font-weight: bold; margin: 8px 0;'>{produto['quantidade']} un</div>
                                <div style='font-size: 12px; background: rgba(255,255,255,0.2); padding: 4px; border-radius: 5px;'>
                                    {produto['porcentagem']}% do total
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                # 🔹 SE HOUVER MAIS DE 6 PRODUTOS, MOSTRAR OS DEMAIS EM LISTA SIMPLES PARA MELHOR LEITURA
                if len(produtos_vendidos) > 6:
                    with st.expander(f"Ver todos os {len(produtos_vendidos)} produtos"):
                        df_expander = produtos_vendidos.iloc[6:].copy()
                        df_expander['Posição'] = range(7, len(produtos_vendidos) + 1)
                        df_expander = df_expander[['Posição', 'nome_produto', 'quantidade', 'porcentagem']]
                        df_expander.columns = ['#', 'Produto', 'Unidade', '% do Total']
                        st.dataframe(df_expander, use_container_width=True, hide_index=True)

            with col_chart6:
                st.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)

                # 🔹 Produtos com maior valor total
                produtos_valor = df_filtrado.groupby('nome_produto').agg({
                    'valor_total_calculado': 'sum'
                }).nlargest(10, 'valor_total_calculado').reset_index()

                # 🔹 FORMATAR VALOR PARA BRASILEIRO NO HOVER
                produtos_valor['valor_formatado'] = produtos_valor['valor_total_calculado'].apply(
                    lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
                )

                # 🔹 Título estilizado
                st.markdown("""
                    <h3 style='
                        text-align: center;
                        font-size: 18px;
                        font-family: Arial, sans-serif;
                    '>
                        Produtos por valor (Total)
                    </h3>
                """, unsafe_allow_html=True)

                # 🔹 Cores estendidas para top 10 produtos
                produtos_valor_colors = ['#9B59B6', '#B8860B', '#2B50E4', '#E67E22', '#27AE60',
                                         '#FF6B6B', '#4ECDC4', '#45B7D1', '#FECA57', '#8B4513']

                # 🔹 Gráfico de barras horizontais
                fig_produtos_valor = px.bar(
                    produtos_valor,
                    x='valor_total_calculado',
                    y='nome_produto',
                    orientation='h',
                    color='nome_produto',
                    color_discrete_sequence=produtos_valor_colors,
                    color_discrete_map={'Teclado Mecanico': '#989795'},
                    labels={'valor_total_calculado': 'Valor Total (R$)', 'nome_produto': ''},
                    text=produtos_valor['valor_formatado']  # 🔹 TEXTO FORMATADO PARA HOVER
                )

                # 🔹 Ajustes no layout para permitir reorganização dinâmica
                fig_produtos_valor.update_layout(
                    yaxis=dict(
                        showticklabels=True,       # ✅ Mostrar rótulos para permitir ajuste
                        title='',                  # 🔹 Sem título no eixo Y
                        automargin=True,
                        categoryorder='total ascending'  # 🔹 Ordenar por valor total para ajuste automático
                    ),
                    showlegend=True,
                    legend=dict(
                        orientation='v',
                        yanchor='top',
                        xanchor='left',
                        x=1.02,
                        itemclick='toggle',
                        itemdoubleclick='toggleothers',
                        font=dict(size=14)
                    ),
                    margin=dict(t=10, b=50, l=50, r=150),
                    height=350,
                    xaxis=dict(
                        tickformat='R$ ,.2f'       # 🔹 FORMATAÇÃO EM REAIS COM 2 CASAS DECIMAIS
                    )
                )

                # 🔹 FORMATAR HOVER PARA BRASILEIRO
                fig_produtos_valor.update_traces(
                    hovertemplate='<b>%{y}</b><br>%{text}<extra></extra>',
                    textposition='none'  # 🔹 NÃO MOSTRAR TEXTO NAS BARRAS
                )

                # 🔹 Remover bordas das barras para visual mais limpo
                fig_produtos_valor.update_traces(
                    marker_line_width=0
                )

                # 🔹 Exibir gráfico no Streamlit
                st.plotly_chart(fig_produtos_valor, use_container_width=True)

    # 🔹 Tabela filtrada com expander e download
    if not df_filtrado.empty:
        with st.expander("Dados Filtrados", expanded=False):

            cols_para_mostrar = [c for c in df_filtrado.columns if c not in ["id_venda", "index", "erros", "data_importacao", "data_registro", "data_compra_dt", "data_venda_dt", "data_compra"]]
            if "id_cliente" in cols_para_mostrar:
                cols_para_mostrar.remove("id_cliente")
                cols_para_mostrar = ["id_cliente"] + cols_para_mostrar

            # Adicionar valor_total_calculado como valor_total para exibição
            df_filtrado_display = df_filtrado[cols_para_mostrar].copy()
            df_filtrado_display['valor_total'] = df_filtrado['valor_total_calculado']

            # Tratamento de dados: remover acentuação da coluna endereco
            if "endereco" in df_filtrado_display.columns:
                df_filtrado_display["endereco"] = df_filtrado_display["endereco"].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else x)

            # Tratamento de dados: primeira letra maiúscula e demais minúsculas na coluna status_venda
            if "status_venda" in df_filtrado_display.columns:
                df_filtrado_display["status_venda"] = df_filtrado_display["status_venda"].apply(lambda x: str(x).capitalize() if pd.notna(x) else x)

            # Garantir formato das datas brasileiras
            for coluna in ["data_compra_dt", "data_venda_dt"]:
                if coluna in df_filtrado_display.columns:
                    df_filtrado_display[coluna] = pd.to_datetime(df_filtrado_display[coluna], errors="coerce").dt.strftime("%d/%m/%Y")

            for coluna in ["data_compra", "data_venda", "data_nascimento"]:
                if coluna in df_filtrado_display.columns:
                    df_filtrado_display[coluna] = df_filtrado_display[coluna].astype(str)

            # Formatar valor_total para exibição em reais
            if "valor_total" in df_filtrado_display.columns:
                df_filtrado_display["valor_total"] = df_filtrado_display["valor_total"].apply(
                    lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.') if pd.notna(x) else ""
                )

            st.dataframe(df_filtrado_display, use_container_width=True)
            st.download_button(
                label="Exportar dados filtrados",
                data=df_filtrado_display.to_csv(index=False, sep=';').encode("utf-8"),
                file_name="dados_filtrados.csv",
                mime="text/csv",
                use_container_width=True
            )

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

# 🔹 Configurações do Supabase (com fallback para Streamlit Cloud)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Fallback para Streamlit Cloud secrets
if not SUPABASE_URL:
    try:
        SUPABASE_URL = st.secrets["supabase"]["url"]
    except (KeyError, TypeError):
        SUPABASE_URL = None

if not SUPABASE_KEY:
    try:
        SUPABASE_KEY = st.secrets["supabase"]["key"]
    except (KeyError, TypeError):
        SUPABASE_KEY = None

supabase = None
try:
    from supabase import create_client
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        st.success("✅ Conectado ao Supabase")
    else:
        st.warning("⚠️ Conexão Supabase não disponível: credenciais não encontradas")
        supabase = None
except Exception as e:
    st.warning(f"⚠️ Conexão Supabase não disponível: {e}")
    supabase = None

# 🔹 Adiciona o diretório pai de 'src' ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 🔹 Importações do seu projeto
from src.pipeline import executar_pipeline, validar_e_padronizar_csv
from src.validacao import corrigir_linha, validar_linha
from src.db_utils import criar_tabela, carregar_usuarios, salvar_usuario, deletar_usuario

# Configurações do dashboard
st.set_page_config(page_title="Painel de Vendas", layout="wide")
REPORTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "reports"))
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "db", "vendas.db"))
USERS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "users.json"))

# Criar tabelas se necessário
criar_tabela()

# Initialize usuarios in session state
if 'usuarios' not in st.session_state:
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
                "loja": "Todas lojas",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": True,
                    "upload_csv": True
                },
                "ativo": True
            },
            "rcirne": {
                "password": "rcirne",
                "role": "admin",
                "nome": "Rafael Cirne",
                "loja": "Todas lojas",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": True,
                    "upload_csv": True
                },
                "ativo": True
            },
            "baronem": {
                "password": "baronem",
                "role": "manager",
                "nome": "Barone Mendes",
                "loja": "Loja Bairro",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "antonios": {
                "password": "antonios",
                "role": "manager",
                "nome": "Antonio Santos",
                "loja": "Loja Shopping",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "josouza": {
                "password": "josouza",
                "role": "user",
                "nome": "João Souza",
                "loja": "Loja Shopping",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "thiagoc": {
                "password": "thiagoc",
                "role": "user",
                "nome": "Thiago Costa",
                "loja": "Loja Bairro",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "csilva": {
                "password": "csilva",
                "role": "manager",
                "nome": "Carlos Silva",
                "loja": "Loja Centro",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": True,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "mnogueira": {
                "password": "mnogueira",
                "role": "user",
                "nome": "Mackenzie Nogueira",
                "loja": "Loja Shopping",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            },
            "maoliveira": {
                "password": "maoliveira",
                "role": "user",
                "nome": "Maria Oliveira",
                "loja": "Loja Centro",
                "permissions": {
                    "ver_filtros": True,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                },
                "ativo": True
            }
        }

            with open(USERS_FILE, "w") as f:
                json.dump(default_users, f, indent=4)
            usuarios = default_users

            # Salvar usuários padrão no banco se não existirem
            for login, data in usuarios.items():
                salvar_usuario(
                    login=login,
                    password=data["password"],
                    role=data["role"],
                    nome=data["nome"],
                    loja=data["loja"],
                    permissions=data["permissions"],
                    codigo_vendedor=data.get("codigo_vendedor"),
                    ativo=data.get("ativo", True)
                )

    # Ensure all default users are in DB (with lowercase logins)
    default_users = {
        "admin": {
            "password": "senha123",
            "role": "admin",
            "nome": "Administrador",
            "loja": "Todas lojas",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": True,
                "analisar_todas_lojas": True,
                "upload_csv": True
            },
            "ativo": True
        },
        "rcirne": {
            "password": "rcirne",
            "role": "admin",
            "nome": "Rafael Cirne",
            "loja": "Todas lojas",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": True,
                "analisar_todas_lojas": True,
                "upload_csv": True
            },
            "ativo": True
        },
        "baronem": {
            "password": "baronem",
            "role": "manager",
            "nome": "Barone Mendes",
            "loja": "Loja Bairro",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": True,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "antonios": {
            "password": "antonios",
            "role": "manager",
            "nome": "Antonio Santos",
            "loja": "Loja Shopping",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": True,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "josouza": {
            "password": "josouza",
            "role": "user",
            "nome": "João Souza",
            "loja": "Loja Shopping",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": False,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "thiagoc": {
            "password": "thiagoc",
            "role": "user",
            "nome": "Thiago Costa",
            "loja": "Loja Bairro",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": False,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "csilva": {
            "password": "csilva",
            "role": "manager",
            "nome": "Carlos Silva",
            "loja": "Loja Centro",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": True,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "mnogueira": {
            "password": "mnogueira",
            "role": "user",
            "nome": "Mackenzie Nogueira",
            "loja": "Loja Shopping",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": False,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        },
        "maoliveira": {
            "password": "maoliveira",
            "role": "user",
            "nome": "Maria Oliveira",
            "loja": "Loja Centro",
            "permissions": {
                "ver_filtros": True,
                "ver_indicadores": True,
                "ver_graficos": True,
                "executar_pipeline": False,
                "analisar_todas_lojas": False,
                "upload_csv": False
            },
            "ativo": True
        }
    }

    for login, data in default_users.items():
        if login not in usuarios:
            salvar_usuario(
                login=login,
                password=data["password"],
                role=data["role"],
                nome=data["nome"],
                loja=data["loja"],
                permissions=data["permissions"],
                codigo_vendedor=data.get("codigo_vendedor"),
                ativo=data.get("ativo", True)
            )

    # Reload usuarios after ensuring
    usuarios = carregar_usuarios()
    st.session_state['usuarios'] = usuarios
else:
    usuarios = st.session_state['usuarios']

# Initialize session state for permissions and form fields
if 'permissions' not in st.session_state:
    st.session_state.permissions = {}
if 'novo_login' not in st.session_state:
    st.session_state.novo_login = ''
if 'novo_nome' not in st.session_state:
    st.session_state.novo_nome = ''
if 'nova_senha' not in st.session_state:
    st.session_state.nova_senha = ''
if 'nova_loja' not in st.session_state:
    st.session_state.nova_loja = ''
if 'novo_role' not in st.session_state:
    st.session_state.novo_role = 'user'
if 'novo_ver_filtros' not in st.session_state:
    st.session_state.novo_ver_filtros = False
if 'novo_ver_indicadores' not in st.session_state:
    st.session_state.novo_ver_indicadores = True
if 'novo_ver_graficos' not in st.session_state:
    st.session_state.novo_ver_graficos = True
if 'novo_executar_pipeline' not in st.session_state:
    st.session_state.novo_executar_pipeline = False
if 'novo_analisar_todas_lojas' not in st.session_state:
    st.session_state.novo_analisar_todas_lojas = False
if 'novo_upload_csv' not in st.session_state:
    st.session_state.novo_upload_csv = False
if 'check_login_input' not in st.session_state:
    st.session_state.check_login_input = ''

# Configure web logger to separate Streamlit logs from batch logs
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'logs')
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
            usuario_lower = usuario.lower()
            if usuario_lower in usuarios and senha == usuarios[usuario_lower]["password"]:
                # Verificar se usuário está ativo
                if not usuarios[usuario_lower].get("ativo", True):
                    st.error("❌ Conta desativada. Entre em contato com o administrador.")
                    st.stop()

                st.session_state.autenticado = True
                st.session_state.usuario = usuario_lower
                st.session_state.role = usuarios[usuario_lower]["role"]
                st.session_state.permissions = usuarios[usuario_lower]["permissions"]
                st.session_state.nome_usuario = usuarios[usuario_lower]["nome"]
                st.session_state.loja_usuario = usuarios[usuario_lower]["loja"]
                st.session_state.codigo_vendedor = usuarios[usuario_lower].get("codigo_vendedor")
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

# Initialize df_filtrado
df_filtrado = pd.DataFrame()

# 🔹 Seção de Configuração
if st.session_state.get("show_config", False):
    st.header("Configuração de Usuários")
    
    # 🔹 SEÇÃO PARA ADICIONAR NOVO USUÁRIO
    st.markdown("---")
    st.subheader("➕ Adicionar Novo Usuário")
    
    # Verificar login fora do form
    col_check1, col_check2 = st.columns([3, 1])
    with col_check1:
        check_login_input = st.text_input("Verificar disponibilidade de login", key="check_login_input", value=st.session_state.get('check_login_input', ''))
    with col_check2:
        if st.button("Verificar", key="check_login_button"):
            if check_login_input:
                if check_login_input in usuarios:
                    st.error("❌ Login existente, digite novamente!")
                    st.session_state['novo_login'] = ""
                else:
                    st.success("✅ Login disponível!")
                    st.session_state['novo_login'] = check_login_input
            else:
                st.warning("Digite um login primeiro.")
                st.session_state['novo_login'] = ""

    with st.form("novo_usuario_form"):
        col1, col2 = st.columns(2)
        with col1:
            novo_login = st.text_input("Login do usuário*", value=st.session_state.get('novo_login', ''), key="novo_login_input")
            novo_nome = st.text_input("Nome completo*", value=st.session_state.get('novo_nome', ''), key="novo_nome_input")
        with col2:
            nova_senha = st.text_input("Senha*", type="password", value=st.session_state.get('nova_senha', ''), key="nova_senha_input")
            # Obter lista de lojas do banco de dados
            lojas_disponiveis = obter_lojas()
            lojas_disponiveis.insert(0, "Todas lojas")
            nova_loja = st.selectbox("Loja de atuação*", options=lojas_disponiveis, index=lojas_disponiveis.index(st.session_state.get('nova_loja', lojas_disponiveis[0])) if st.session_state.get('nova_loja') in lojas_disponiveis else 0, key="nova_loja_select")



        st.markdown("**Permissões:**")
        col3, col4 = st.columns(2)
        with col3:
            novo_role = st.selectbox("Perfil*", ["user", "admin", "manager"], index=["user", "admin", "manager"].index(st.session_state.get('novo_role', 'user')), key="novo_role_select")
            novo_ver_filtros = st.checkbox("Ver Filtros", value=st.session_state.get('novo_ver_filtros', False), key="novo_ver_filtros_check")
            novo_ver_indicadores = st.checkbox("Ver Indicadores", value=st.session_state.get('novo_ver_indicadores', True), key="novo_ver_indicadores_check")
            novo_ver_graficos = st.checkbox("Ver Gráficos", value=st.session_state.get('novo_ver_graficos', True), key="novo_ver_graficos_check")
        with col4:
            novo_executar_pipeline = st.checkbox("Executar Pipeline", value=st.session_state.get('novo_executar_pipeline', False), key="novo_executar_pipeline_check")
            novo_analisar_todas_lojas = st.checkbox("Analisar Todas as lojas", value=st.session_state.get('novo_analisar_todas_lojas', False), key="novo_analisar_todas_lojas_check")
            novo_upload_csv = st.checkbox("Upload CSV", value=st.session_state.get('novo_upload_csv', False), key="novo_upload_csv_check")

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
                        "executar_pipeline": True,
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

                # Converter login para minúsculas
                novo_login_lower = novo_login.lower()

                novo_usuario = {
                    "password": nova_senha,
                    "role": novo_role,
                    "nome": novo_nome,
                    "loja": nova_loja,
                    "permissions": permissions_padrao
                }

                # Salvar no banco de dados com login em minúsculas
                salvar_usuario(
                    login=novo_login_lower,
                    password=nova_senha,
                    role=novo_role,
                    nome=novo_nome,
                    loja=nova_loja,
                    permissions=novo_usuario["permissions"]
                )

                # Atualizar lista local com login em minúsculas
                usuarios[novo_login_lower] = novo_usuario
                st.session_state['usuarios'] = usuarios
                st.success(f"✅ Usuário '{novo_login_lower}' adicionado com sucesso!")

                # Limpar formulário após sucesso
                st.session_state.novo_login = ""
                st.session_state.novo_nome = ""
                st.session_state.nova_senha = ""
                st.session_state.nova_loja = lojas_disponiveis[0] if lojas_disponiveis else ""
                st.session_state.novo_role = "user"
                st.session_state.novo_ver_filtros = False
                st.session_state.novo_ver_indicadores = True
                st.session_state.novo_ver_graficos = True
                st.session_state.novo_executar_pipeline = False
                st.session_state.novo_analisar_todas_lojas = False
                st.session_state.novo_upload_csv = False
                st.rerun()

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
                    lojas_disponiveis.insert(0, "Todas lojas")
                    loja_atual = data.get("loja", "Todas lojas")
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
                        novo_executar_pipeline = True
                        novo_analisar_todas_lojas = False
                        novo_upload_csv = False
                    else:
                        # Para admin e user, permitir edição
                        novo_ver_filtros = st.checkbox("Ver Filtros", value=perms.get("ver_filtros", True), key=f"filtros_{user}_config")
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
                        st.session_state['usuarios'] = usuarios

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
                                st.session_state['usuarios'] = usuarios
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
                                st.session_state['usuarios'] = usuarios
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
                usuarios_atualizados = carregar_usuarios()
                st.session_state['usuarios'] = usuarios_atualizados
                st.success("Usuários carregados do banco com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erro ao recarregar usuários: {e}")

        if st.button("Voltar ao Dashboard", use_container_width=True, key="voltar_dashboard"):
            st.session_state.show_config = False
            st.rerun()

# 🔹 Dashboard Principal (quando não está no modo configuração)
else:
    # Inicializar df
    df = None

    # 🔹 Carregamento automático dos dados do CSV após login
    data_loaded_from_csv = False
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "vendas_clean.csv")
    if os.path.exists(csv_path):
        st.info("Carregando dados automaticamente do arquivo CSV...")
        try:
            # Carregar e processar CSV automaticamente
            df_csv = pd.read_csv(csv_path, sep=",", dtype=str)

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

            data_loaded_from_csv = True
            st.session_state['data_processed'] = True
            st.success("✅ Dados carregados automaticamente do CSV!")
        except Exception as e:
            st.error(f"❌ Erro ao carregar CSV automaticamente: {e}")
            df = carregar_dados_sqlite()
            st.session_state['data_processed'] = False
    else:
        df = carregar_dados_sqlite()
        st.session_state['data_processed'] = False

    # 🔹 Upload ou carregamento adicional (opcional)
    with st.expander("Importar Dados Adicionais", expanded=False):
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
            df = pd.read_csv(uploaded_file, sep=separador, dtype=str)

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
        if not data_loaded_from_csv:
            #st.subheader("Carregando dados")
            df = carregar_dados_sqlite()

            # Se não há dados no banco, tentar carregar automaticamente do CSV
            if df.empty:
                csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "vendas_clean.csv")
                if os.path.exists(csv_path):
                    st.info("🔄 Carregando dados automaticamente do arquivo CSV...")
                    try:
                        # Carregar e processar CSV automaticamente
                        df_csv = pd.read_csv(csv_path, sep=",", dtype=str)

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

    # Converter colunas numéricas
    df_corrigido["quantidade"] = pd.to_numeric(df_corrigido["quantidade"], errors="coerce").fillna(0).astype(int)
    df_corrigido["valor_produto"] = pd.to_numeric(df_corrigido["valor_produto"], errors="coerce").fillna(0.0).astype(float)

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

    # 🔹 CORREÇÃO CRÍTICA: Definir filtros baseados no perfil do usuário
    if st.session_state.role == "user":
        # 🔹 USER: vê apenas dados do PRÓPRIO usuário (nome_vendedor = nome_usuario)
        filtro_loja = [st.session_state.loja_usuario]
        # Apenas vendas onde o nome do vendedor é igual ao nome do usuário logado
        filtro_vendedor = [st.session_state.nome_usuario]
        
    elif st.session_state.role == "manager" and st.session_state.loja_usuario != "Todas lojas":
        # 🔹 MANAGER: vê dados de TODOS os vendedores da SUA loja
        filtro_loja = [st.session_state.loja_usuario]
        filtro_vendedor = df_corrigido[df_corrigido["nome_loja"] == st.session_state.loja_usuario]["nome_vendedor"].dropna().unique()
    else:
        # 🔹 ADMIN ou manager com "Todas" as lojas
        filtro_loja = df_corrigido["nome_loja"].dropna().unique()
        filtro_vendedor = df_corrigido["nome_vendedor"].dropna().unique()

    # 🔹 Filtros adicionais (sempre aplicados, mas UI só se tiver permissão)
    filtro_pagamento = df_corrigido["forma_pagamento"].dropna().unique()
    filtro_produto = df_corrigido["nome_produto"].dropna().unique()
    filtro_status_usuario = ["Ativo"]  # Por padrão, mostrar apenas ativos

    # 🔹 Se tem permissão para ver filtros, mostrar interface para ajustar
    if st.session_state.permissions.get("ver_filtros", True):
        # Para USER, não permitir ajustar vendedor - já está fixo no próprio nome
        if st.session_state.role == "user":
            st.sidebar.info(f"Vendedor: {st.session_state.nome_usuario}")
            # Não mostrar multiselect para user
            
        # Para manager, permitir ajustar vendedores dentro da loja
        elif st.session_state.role == "manager" and st.session_state.loja_usuario != "Todas lojas":
            filtro_vendedor = st.sidebar.multiselect("Vendedor",
                df_corrigido[df_corrigido["nome_loja"] == st.session_state.loja_usuario]["nome_vendedor"].dropna().unique(),
                default=filtro_vendedor)
        elif st.session_state.role in ["admin", "manager"]:
            # Admin/manager podem ajustar loja e vendedor
            filtro_loja = st.sidebar.multiselect("Loja", df_corrigido["nome_loja"].dropna().unique(),
                                                 default=filtro_loja)
            filtro_vendedor = st.sidebar.multiselect("Vendedor", df_corrigido["nome_vendedor"].dropna().unique(),
                                                     default=filtro_vendedor)

        # Filtros adicionais para admin/manager
        if st.session_state.role in ["admin", "manager"]:
            filtro_pagamento = st.sidebar.multiselect("Forma de Pagamento", df_corrigido["forma_pagamento"].dropna().unique(),
                                                      default=filtro_pagamento)
            filtro_produto = st.sidebar.multiselect("Produto", df_corrigido["nome_produto"].dropna().unique(),
                                                   default=filtro_produto)

            filtro_status_usuario = st.sidebar.multiselect(
                "Status do Usuário",
                ["Ativo", "Desativado"],
                default=filtro_status_usuario,
                help="Filtrar vendas por status do usuário responsável"
            )

    # 🔹 Filtro de datas (data_venda) - formato brasileiro
    data_min = df_corrigido["data_venda_dt"].min().date()
    data_max = df_corrigido["data_venda_dt"].max().date()
    inicio = st.sidebar.date_input("Data inicial - Venda (dd/mm/aaaa)", value=data_min, format="DD/MM/YYYY")
    fim = st.sidebar.date_input("Data final - Venda (dd/mm/aaaa)", value=data_max, format="DD/MM/YYYY")
    st.sidebar.caption("Calendário e datas no padrão brasileiro: dia/mês/ano. Se o calendário aparecer em inglês, ajuste o idioma do navegador para português.")

    # 🔹 Aplicar filtros - CORREÇÃO: Simplificar lógica de filtragem
    try:
        # Aplicar filtros básicos para todos os usuários
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

    except Exception as e:
        st.error(f"❌ Erro ao aplicar filtros: {e}")
        # Fallback: usar dados não filtrados
        df_filtrado = df_corrigido.copy()

    # 🔹 VERIFICAR SE df_filtrado EXISTE ANTES DE USAR
    if 'df_filtrado' not in locals() or df_filtrado.empty:
        st.info("Nenhum dado encontrado com os filtros aplicados. Verifique os filtros selecionados.")
        # Usar df_corrigido como fallback para evitar erros
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
                else:
                    st.metric("Loja Destaque", "N/A")
            
            with card3:
                if not vendas_por_produto.empty:
                    produto_top = vendas_por_produto.index[0]
                    valor_produto = vendas_por_produto.iloc[0]
                    st.metric("Campeão de Vendas", produto_top, delta=f"R$ {valor_produto:,.2f}")
                else:
                    st.metric("Campeão de Vendas", "N/A")
            
            with card4:
                if not vendas_por_vendedor.empty:
                    vendedor_top = vendas_por_vendedor.index[0]
                    valor_vendedor = vendas_por_vendedor.iloc[0]
                    st.metric("Vendedor Destaque", vendedor_top, delta=f"R$ {valor_vendedor:,.2f}")
                else:
                    st.metric("Vendedor Destaque", "N/A")
            
            with card5:
                st.metric("Média por Transação", f"R$ {ticket_medio:,.2f}")

            nova_linha()
            
        except Exception as e:
            st.error(f"❌ Erro ao calcular indicadores: {e}")

    # 🔹 Gráficos Interativos - CORREÇÃO: Simplificar lógica e garantir que apareçam
    if st.session_state.permissions.get("ver_graficos", True):
        st.subheader("Insights Interativos")
        
        # 🔹 PALETAS DE CORES DEFINIDAS
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
        
        # 🔹 DEFINIR AS TABS
        tab1, tab2, tab3 = st.tabs(["Por Loja", "Evolução Temporal", "Por Produto"])
        
        with tab1:
            col_chart1, col_chart2 = st.columns(2)

            with col_chart1:
                # Vendas por loja
                if not df_filtrado.empty and 'nome_loja' in df_filtrado.columns:
                    vendas_loja = df_filtrado.groupby('nome_loja').agg({
                        'valor_total_calculado': 'sum'
                    }).reset_index()

                    if not vendas_loja.empty:
                        # Criar coluna com nome curto da loja
                        vendas_loja['nome_loja_curto'] = vendas_loja['nome_loja'].str.replace('Loja ', '', regex=False)
                        vendas_loja['valor_formatado'] = vendas_loja['valor_total_calculado'].apply(
                            lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
                        )
                        vendas_loja['nome_loja_display'] = vendas_loja['nome_loja'].map(loja_display_map)

                        fig_lojas = px.bar(
                            vendas_loja,
                            x='nome_loja_display',
                            y='valor_total_calculado',
                            title="Vendas por Loja",
                            color='nome_loja',
                            color_discrete_map=loja_color_map,
                            labels={'nome_loja_display': 'Loja', 'valor_total_calculado': 'Valor Total (R$)'},
                            text=vendas_loja['valor_formatado']
                        )

                        fig_lojas.update_layout(
                            title={'text': "Vendas por loja", 'x': 0.5, 'xanchor': 'center'},
                            yaxis=dict(tickformat='R$ ,.2f'),
                            legend_title_text=None,
                            legend=dict(font=dict(size=14))
                        )

                        fig_lojas.update_traces(
                            hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
                            textposition='none'
                        )

                        st.plotly_chart(fig_lojas, use_container_width=True)
                    else:
                        st.info("📊 Não há dados de vendas por loja para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir gráfico de vendas por loja.")

            with col_chart2:
                # Top vendedores
                if not df_filtrado.empty and 'nome_vendedor' in df_filtrado.columns and 'nome_loja' in df_filtrado.columns:
                    top_vendedores_com_loja = df_filtrado.groupby(['nome_vendedor', 'nome_loja']).agg({
                        'valor_total_calculado': 'sum'
                    }).reset_index().nlargest(10, 'valor_total_calculado')

                    if not top_vendedores_com_loja.empty:
                        top_vendedores_com_loja['nome_loja_curto'] = top_vendedores_com_loja['nome_loja'].str.replace('Loja ', '', regex=False)
                        top_vendedores_com_loja['valor_formatado'] = top_vendedores_com_loja['valor_total_calculado'].apply(
                            lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
                        )
                        top_vendedores_com_loja['nome_loja_display'] = top_vendedores_com_loja['nome_loja'].map(loja_display_map)
                        top_vendedores_com_loja = top_vendedores_com_loja.sort_values('valor_total_calculado', ascending=True)

                        fig_vendedores = px.bar(
                            top_vendedores_com_loja,
                            x='valor_total_calculado',
                            y='nome_vendedor',
                            orientation='h',
                            title="Total por vendedor",
                            color='nome_loja',
                            color_discrete_map=loja_color_map,
                            labels={'valor_total_calculado': 'Valor Total (R$)', 'nome_vendedor': 'Vendedor'},
                            text=top_vendedores_com_loja['valor_formatado']
                        )

                        fig_vendedores.update_layout(
                            yaxis={'categoryorder': 'total ascending'},
                            title={'text': "Total por vendedor", 'x': 0.5, 'xanchor': 'center'},
                            xaxis=dict(tickformat='R$ ,.2f'),
                            legend_title_text=None,
                            legend=dict(font=dict(size=14))
                        )

                        fig_vendedores.update_traces(
                            hovertemplate='<b>%{y}</b><br>%{text}<br>Loja: %{fullData.name}<extra></extra>',
                            textposition='none'
                        )

                        st.plotly_chart(fig_vendedores, use_container_width=True)
                    else:
                        st.info("📊 Não há dados de vendedores para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir gráfico de vendedores.")

        with tab2:
            col_chart3, col_chart4 = st.columns(2)

            with col_chart3:
                # Evolução temporal
                if not df_filtrado.empty and 'data_venda_dt' in df_filtrado.columns and 'nome_loja' in df_filtrado.columns:
                    df_filtrado_copy = df_filtrado.copy()
                    df_filtrado_copy['month'] = df_filtrado_copy['data_venda_dt'].dt.month
                    df_filtrado_copy['year'] = df_filtrado_copy['data_venda_dt'].dt.year

                    evolucao_lojas = df_filtrado_copy.groupby(['year', 'month', 'nome_loja']).agg({
                        'valor_total_calculado': 'sum'
                    }).reset_index()

                    if not evolucao_lojas.empty:
                        evolucao_lojas = evolucao_lojas.sort_values(['year', 'month', 'valor_total_calculado'])
                        evolucao_lojas['nome_loja_curto'] = evolucao_lojas['nome_loja'].str.replace('Loja ', '', regex=False)
                        evolucao_lojas['nome_loja_display'] = evolucao_lojas['nome_loja'].map(loja_display_map)

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
                            color_discrete_map=loja_color_map,
                            labels={'data_display': 'Data', 'valor_total_calculado': 'Valor Total (R$)'}
                        )

                        fig_evolucao.update_layout(
                            hovermode='x unified',
                            showlegend=True,
                            barmode='stack',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1,
                                itemclick="toggle",
                                itemdoubleclick="toggleothers",
                                font=dict(size=14)
                            ),
                            yaxis=dict(tickformat='R$ ,.2f'),
                            xaxis=dict(categoryorder='array', categoryarray=evolucao_lojas['data_display'].unique()),
                            legend_title_text=None
                        )

                        fig_evolucao.update_traces(
                            hovertemplate='<b>%{fullData.name}</b><br>Data: %{x}<br>Valor: R$ %{y:,.2f}<extra></extra>'
                        )

                        fig_evolucao.update_layout(legend_traceorder='reversed')
                        st.plotly_chart(fig_evolucao, use_container_width=True)
                    else:
                        st.info("📊 Não há dados de evolução temporal para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir gráfico de evolução temporal.")

            with col_chart4:
                # Formas de pagamento
                if not df_filtrado.empty and 'forma_pagamento' in df_filtrado.columns:
                    pagamentos = df_filtrado.groupby('forma_pagamento').agg({
                        'valor_total_calculado': 'sum',
                        'quantidade': 'count'
                    }).reset_index()

                    if not pagamentos.empty:
                        total_valor = pagamentos['valor_total_calculado'].sum()
                        pagamentos['porcentagem'] = (pagamentos['valor_total_calculado'] / total_valor * 100).round(1)
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

                        fig_pagamentos.update_traces(
                            textposition='outside',
                            texttemplate='<b>%{customdata[0]}</b><br><b>(%{customdata[1]}%)</b>',
                            hovertemplate='<b>%{label}</b><br>Valor: %{customdata[0]}<br>Percentual: %{customdata[1]:.1f}%',
                            marker=dict(line=dict(color='#ffffff', width=2))
                        )

                        fig_pagamentos.update_layout(
                            title={'text': "Distribuição por forma de pagamento", 'x': 0.5, 'xanchor': 'center'},
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
                            uniformtext_minsize=13.5,
                            uniformtext_mode='hide'
                        )

                        st.plotly_chart(fig_pagamentos, use_container_width=True)
                    else:
                        st.info("📊 Não há dados de formas de pagamento para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir gráfico de formas de pagamento.")
        
        with tab3:
            col_chart5, col_chart6 = st.columns(2)
            
            with col_chart5:
                # Produtos mais vendidos
                if not df_filtrado.empty and 'nome_produto' in df_filtrado.columns:
                    produtos_vendidos = df_filtrado.groupby('nome_produto').agg({
                        'quantidade': 'sum'
                    }).nlargest(10, 'quantidade').reset_index()

                    if not produtos_vendidos.empty:
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

                        if len(produtos_vendidos) > 6:
                            with st.expander(f"Ver todos os {len(produtos_vendidos)} produtos"):
                                df_expander = produtos_vendidos.iloc[6:].copy()
                                df_expander['Posição'] = range(7, len(produtos_vendidos) + 1)
                                df_expander = df_expander[['Posição', 'nome_produto', 'quantidade', 'porcentagem']]
                                df_expander.columns = ['#', 'Produto', 'Unidade', '% do Total']
                                st.dataframe(df_expander, use_container_width=True, hide_index=True)
                    else:
                        st.info("📊 Não há dados de produtos vendidos para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir cards de produtos mais vendidos.")

            with col_chart6:
                st.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)

                # Produtos por valor
                if not df_filtrado.empty and 'nome_produto' in df_filtrado.columns:
                    produtos_valor = df_filtrado.groupby('nome_produto').agg({
                        'valor_total_calculado': 'sum'
                    }).nlargest(10, 'valor_total_calculado').reset_index()

                    if not produtos_valor.empty:
                        produtos_valor['valor_formatado'] = produtos_valor['valor_total_calculado'].apply(
                            lambda x: f"R$ {x:,.2f}".replace(',', 'temp').replace('.', ',').replace('temp', '.')
                        )

                        st.markdown("""
                            <h3 style='
                                text-align: center;
                                font-size: 18px;
                                font-family: Arial, sans-serif;
                            '>
                                Produtos por valor (Total)
                            </h3>
                        """, unsafe_allow_html=True)

                        produtos_valor_colors = ['#9B59B6', '#B8860B', '#2B50E4', '#E67E22', '#27AE60',
                                                 '#FF6B6B', '#4ECDC4', '#45B7D1', '#FECA57', '#8B4513']

                        fig_produtos_valor = px.bar(
                            produtos_valor,
                            x='valor_total_calculado',
                            y='nome_produto',
                            orientation='h',
                            color='nome_produto',
                            color_discrete_sequence=produtos_valor_colors,
                            labels={'valor_total_calculado': 'Valor Total (R$)', 'nome_produto': ''},
                            text=produtos_valor['valor_formatado']
                        )

                        fig_produtos_valor.update_layout(
                            yaxis=dict(
                                showticklabels=True,
                                title='',
                                automargin=True,
                                categoryorder='total ascending'
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
                            xaxis=dict(tickformat='R$ ,.2f')
                        )

                        fig_produtos_valor.update_traces(
                            hovertemplate='<b>%{y}</b><br>%{text}<extra></extra>',
                            textposition='none',
                            marker_line_width=0
                        )

                        st.plotly_chart(fig_produtos_valor, use_container_width=True)
                    else:
                        st.info("📊 Não há dados de produtos por valor para exibir.")
                else:
                    st.info("📊 Aguardando dados para exibir gráfico de produtos por valor.")

    # 🔹 Tabela filtrada com expander e download
    if not df_filtrado.empty:
        with st.expander("Dados Filtrados", expanded=False):
            cols_para_mostrar = [c for c in df_filtrado.columns if c not in ["id_venda", "index", "erros", "data_importacao", "data_registro", "data_compra_dt", "data_venda_dt", "data_compra", "valor_total_calculado"]]
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
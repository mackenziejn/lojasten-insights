"""
Módulo de gerenciamento de usuários e permissões.
"""
import json
import os
import hashlib
from typing import Dict, List, Optional

USERS_FILE = "data/users.json"

# Perfis disponíveis e suas permissões
PERFIS = {
    "admin": {
        "nome": "Administrador",
        "pode_editar_usuarios": True,
        "pode_executar_pipeline": True,
        "pode_exportar_dados": True,
        "pode_ver_todos_graficos": True,
        "pode_ver_tabela_completa": True,
        "pode_filtrar_lojas": True,
        "pode_filtrar_vendedores": True,
        "pode_ver_indicadores": True,
        "pode_enviar_dropbox": True
    },
    "manager": {
        "nome": "Gerente",
        "pode_editar_usuarios": False,
        "pode_executar_pipeline": True,
        "pode_exportar_dados": True,
        "pode_ver_todos_graficos": True,
        "pode_ver_tabela_completa": True,
        "pode_filtrar_lojas": True,
        "pode_filtrar_vendedores": True,
        "pode_ver_indicadores": True,
        "pode_enviar_dropbox": False
    },
    "user": {
        "nome": "Usuário",
        "pode_editar_usuarios": False,
        "pode_executar_pipeline": False,
        "pode_exportar_dados": False,
        "pode_ver_todos_graficos": True,
        "pode_ver_tabela_completa": False,
        "pode_filtrar_lojas": False,
        "pode_filtrar_vendedores": False,
        "pode_ver_indicadores": True,
        "pode_enviar_dropbox": False
    },
    "visualizador": {
        "nome": "Visualizador",
        "pode_editar_usuarios": False,
        "pode_executar_pipeline": False,
        "pode_exportar_dados": False,
        "pode_ver_todos_graficos": True,
        "pode_ver_tabela_completa": False,
        "pode_filtrar_lojas": False,
        "pode_filtrar_vendedores": False,
        "pode_ver_indicadores": True,
        "pode_enviar_dropbox": False
    }
}

def hash_senha(senha: str) -> str:
    """Gera hash SHA256 da senha."""
    return hashlib.sha256(senha.encode()).hexdigest()

def carregar_usuarios() -> Dict:
    """Carrega usuários do arquivo JSON."""
    if not os.path.exists(USERS_FILE):
        # Criar arquivo inicial com usuários padrão
        usuarios_padrao = {
            "admin": {
                "senha": hash_senha("senha123"),
                "perfil": "admin",
                "nome_completo": "Administrador do Sistema"
            },
            "mackenzie": {
                "senha": hash_senha("vendas2025"),
                "perfil": "gerente",
                "nome_completo": "Mackenzie Gerente"
            }
        }
        salvar_usuarios(usuarios_padrao)
        return usuarios_padrao
    
    with open(USERS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_usuarios(usuarios: Dict):
    """Salva usuários no arquivo JSON."""
    os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(usuarios, f, indent=2, ensure_ascii=False)

def autenticar(usuario: str, senha: str) -> Optional[Dict]:
    """
    Autentica usuário e retorna dados do usuário se válido.
    Retorna None se inválido.
    """
    usuarios = carregar_usuarios()
    if usuario in usuarios:
        senha_hash = hash_senha(senha)
        if usuarios[usuario]["senha"] == senha_hash:
            return {
                "usuario": usuario,
                "perfil": usuarios[usuario]["perfil"],
                "nome_completo": usuarios[usuario].get("nome_completo", usuario),
                "permissoes": PERFIS.get(usuarios[usuario]["perfil"], PERFIS["visualizador"])
            }
    return None

def adicionar_usuario(usuario: str, senha: str, perfil: str, nome_completo: str) -> bool:
    """Adiciona novo usuário."""
    if perfil not in PERFIS:
        return False
    
    usuarios = carregar_usuarios()
    usuarios[usuario] = {
        "senha": hash_senha(senha),
        "perfil": perfil,
        "nome_completo": nome_completo
    }
    salvar_usuarios(usuarios)
    return True

def remover_usuario(usuario: str) -> bool:
    """Remove usuário (não pode remover admin)."""
    if usuario == "admin":
        return False
    
    usuarios = carregar_usuarios()
    if usuario in usuarios:
        del usuarios[usuario]
        salvar_usuarios(usuarios)
        return True
    return False

def atualizar_usuario(usuario: str, senha: Optional[str] = None, 
                     perfil: Optional[str] = None, 
                     nome_completo: Optional[str] = None) -> bool:
    """Atualiza dados de um usuário."""
    usuarios = carregar_usuarios()
    if usuario not in usuarios:
        return False
    
    if senha:
        usuarios[usuario]["senha"] = hash_senha(senha)
    if perfil and perfil in PERFIS:
        usuarios[usuario]["perfil"] = perfil
    if nome_completo:
        usuarios[usuario]["nome_completo"] = nome_completo
    
    salvar_usuarios(usuarios)
    return True

def listar_usuarios() -> List[Dict]:
    """Retorna lista de usuários com seus dados (exceto senha)."""
    usuarios = carregar_usuarios()
    resultado = []
    for usuario, dados in usuarios.items():
        resultado.append({
            "usuario": usuario,
            "perfil": dados["perfil"],
            "nome_completo": dados.get("nome_completo", usuario),
            "tipo_perfil": PERFIS[dados["perfil"]]["nome"]
        })
    return resultado

def get_permissoes(perfil: str) -> Dict:
    """Retorna permissões do perfil."""
    return PERFIS.get(perfil, PERFIS["visualizador"])

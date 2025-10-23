# LojasTen Insights 📊

Dashboard interativo para análise de dados de vendas de múltiplas lojas, com sistema de autenticação e permissões.

## 🌟 Funcionalidades

- **📈 Análise Visual**: Gráficos interativos com Plotly
- **🔐 Sistema de Autenticação**: Usuários com perfis e permissões
- **📊 Indicadores KPI**: Valor total, ticket médio, top produtos/lojas/vendedores
- **🔍 Filtros Avançados**: Por loja, vendedor, data, produto, forma de pagamento
- **📤 Exportação**: Dados filtrados em CSV
- **⚙️ Gerenciamento**: Painel admin para usuários e configurações
- **☁️ Integração Dropbox**: Upload automático de relatórios

## 🚀 Deploy na Nuvem

### Streamlit Cloud (Recomendado)

1. **Crie um repositório no GitHub**
   ```bash
   # Clone ou crie seu repo
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/SEU_USUARIO/lojasten-insights.git
   git push -u origin main
   ```

2. **Deploy no Streamlit Cloud**
   - Acesse: https://share.streamlit.io/
   - Conecte sua conta GitHub
   - Selecione o repositório `lojasten-insights`
   - Configure:
     - **Main file**: `dashboard/app.py`
     - **Python version**: 3.9
   - Clique em "Deploy"

3. **URL Personalizada**
   - Seu app ficará em: `https://lojasten.streamlit.app`
   - Para URL personalizada, use um domínio próprio

### Railway (Alternativa)

1. **Deploy no Railway**
   ```bash
   # Instalar Railway CLI
   npm install -g @railway/cli

   # Login e deploy
   railway login
   railway init
   railway up
   ```

2. **Configurar variáveis de ambiente**
   - No painel Railway, adicione variáveis se necessário

## 🛠️ Instalação Local

```bash
# Clone o repositório
git clone https://github.com/SEU_USUARIO/lojasten-insights.git
cd lojasten-insights

# Execute o setup
./setup.sh

# Instale dependências
pip install -r requirements_cloud.txt

# Execute o app
streamlit run dashboard/app.py
```

## 👥 Usuários Padrão

| Usuário | Senha | Perfil | Permissões |
|---------|-------|--------|------------|
| admin | senha123 | Administrador | Todas |
| mackenzie | vendas2025 | Gerente | Limitadas |

## 📁 Estrutura do Projeto

```
Vendas_teste/
├── dashboard/
│   └── app.py                 # App principal Streamlit
├── src/
│   ├── pipeline.py            # Pipeline ETL
│   ├── db_utils.py            # Utilitários banco de dados
│   ├── validacao.py           # Validação e correção de dados
│   ├── gerador_dados.py       # Geração de dados fake
│   ├── users_manager.py       # Gerenciamento de usuários
│   └── dropbox_upload.py      # Integração Dropbox
├── data/
│   ├── db/                    # Banco SQLite
│   ├── raw/                   # Dados brutos
│   ├── processed/             # Dados processados
│   ├── reports/               # Relatórios gerados
│   └── logs/                  # Logs da aplicação
├── .streamlit/
│   ├── config.toml           # Configurações Streamlit
│   └── secrets.toml          # Segredos (API keys, etc.)
├── requirements_cloud.txt     # Dependências para nuvem
├── setup.sh                  # Script de setup
└── README.md                 # Esta documentação
```

## 🔧 Configuração

### Banco de Dados
- **Local**: SQLite (`data/db/vendas.db`)
- **Nuvem**: Persistido automaticamente no Streamlit Cloud

### Autenticação
- Usuários armazenados no banco SQLite
- Perfis: Admin, Manager, User
- Permissões granulares por funcionalidade

### Dropbox (Opcional)
Para habilitar upload de relatórios:
1. Crie um app no [Dropbox App Console](https://www.dropbox.com/developers/apps)
2. Gere um token de acesso
3. Adicione no `.streamlit/secrets.toml`:
   ```toml
   [dropbox]
   token = "SEU_TOKEN_AQUI"
   ```

## 📊 Funcionalidades por Perfil

### 👑 Administrador
- ✅ Todas as funcionalidades
- ✅ Gerenciamento de usuários
- ✅ Execução de pipeline
- ✅ Upload de CSV
- ✅ Análise de todas as lojas

### 👨‍💼 Gerente
- ✅ Filtros e indicadores
- ✅ Gráficos e análises
- ✅ Execução de pipeline
- ❌ Gerenciamento de usuários
- ❌ Upload de CSV

### 👤 Usuário
- ✅ Indicadores básicos
- ✅ Gráficos limitados
- ❌ Filtros avançados
- ❌ Execução de pipeline
- ❌ Upload de CSV

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para detalhes.

## 📞 Suporte

Para suporte, abra uma issue no GitHub ou entre em contato com a equipe de desenvolvimento.

---

**Desenvolvido com ❤️ para análise de dados de vendas**

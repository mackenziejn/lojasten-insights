# Vendas Teste - Pipeline ETL e Dashboard de Vendas

Este projeto implementa um pipeline ETL completo para processamento de dados de vendas, com validação de dados, geração de relatórios e um dashboard interativo em Streamlit para análise de vendas.

## 📋 Visão Geral

O sistema processa dados de vendas de múltiplas lojas, realiza limpeza e validação dos dados, armazena em banco SQLite e fornece insights através de dashboards interativos. Suporta múltiplos usuários com diferentes níveis de permissão.

## 🏗️ Arquitetura

```
Vendas_teste/
├── assets/                 # Recursos visuais (logos, imagens)
├── dashboard/
│   └── app.py             # Dashboard Streamlit principal
├── data/
│   ├── raw/               # Dados brutos de entrada (CSV)
│   ├── processed/         # Dados tratados após processamento
│   ├── archived/          # Dados históricos arquivados
│   ├── reports/           # Relatórios gerados (PDF, CSV)
│   └── db/                # Banco de dados SQLite
├── scripts/               # Scripts utilitários
├── src/                   # Código fonte principal
│   ├── db_utils.py        # Utilitários de banco de dados
│   ├── etl.py            # Extração e transformação de dados
│   ├── pipeline.py       # Pipeline principal de processamento
│   ├── validacao.py      # Validação e correção de dados
│   ├── gerador_dados.py  # Geração de dados de teste
│   └── dropbox_upload.py # Integração com Dropbox
├── tests/                 # Testes automatizados
├── venv_vendas/          # Ambiente virtual (opcional)
├── main.py               # Ponto de entrada CLI
├── requirements.txt      # Dependências Python
├── Makefile             # Comandos de automação
├── run_pipeline.sh      # Script de execução do pipeline
└── README.md            # Esta documentação
```

## 🚀 Instalação e Configuração

### Pré-requisitos

- Python 3.8+
- pip
- SQLite3

### Instalação

1. **Clone o repositório:**
   ```bash
   git clone <repository-url>
   cd Vendas_teste
   ```

2. **Crie e ative o ambiente virtual (recomendado):**
   ```bash
   python3 -m venv venv_vendas
   source venv_vendas/bin/activate  # Linux/Mac
   # ou
   venv_vendas\Scripts\activate     # Windows
   ```

3. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Execute as migrações do banco:**
   ```bash
   python main.py migrate
   ```

## 📊 Uso

### Interface de Linha de Comando (CLI)

O sistema oferece múltiplos comandos via `main.py`:

#### Processamento de Dados
```bash
# Executar pipeline completo
python main.py run

# Executar pipeline com geração de dados de exemplo
python main.py run --generate-sample --sample-size 200

# Apenas gerar dados de exemplo
python main.py generate-sample --sample-size 100

# Validação sem salvar no banco (dry-run)
python main.py dry-run

# Executar migrações do banco
python main.py migrate
```

#### Gerenciamento de Lojas e Vendedores
```bash
# Listar mapeamentos loja-vendedor
python -m src.admin list-mappings [--codigo_loja L01]

# Desbloquear loja
python -m src.admin unlock L01

# Bloquear loja
python -m src.admin lock L01

# Reatribuir vendedor
python -m src.admin reassign-seller L01 V_old V_new
```

### Dashboard Interativo

Execute o dashboard Streamlit:

```bash
streamlit run dashboard/app.py
```

#### Funcionalidades do Dashboard

- **Autenticação:** Sistema de login com usuários e permissões
- **Upload de CSV:** Importação de novos dados de vendas
- **Filtros Avançados:** Por loja, vendedor, data, produto, forma de pagamento
- **Indicadores:** Métricas de vendas, ticket médio, top produtos/lojas/vendedores
- **Gráficos Interativos:** Evolução temporal, distribuição por formas de pagamento, análise por produto
- **Execução de Pipeline:** Possibilidade de executar o processamento diretamente da interface
- **Relatórios:** Geração e download de relatórios em PDF
- **Gerenciamento de Usuários:** Interface para admins gerenciarem usuários e permissões

#### Perfis de Usuário

- **Admin:** Acesso completo a todas as funcionalidades
- **Manager:** Acesso limitado à sua loja, sem upload de CSV
- **User:** Acesso apenas leitura, limitado aos próprios dados

### Automação com Makefile

```bash
# Criar ambiente virtual e instalar dependências
make venv

# Executar testes
make test

# Executar pipeline
make run

# Executar dry-run
make dry-run

# Iniciar dashboard
make dashboard

# Executar migrações
make migrate

# Agregar dados duplicados
make aggregate
```

### Script de Automação

Para execução automatizada em servidores:

```bash
# Executar pipeline completo
./run_pipeline.sh
```

## 🔧 Configuração

### Banco de Dados

O sistema utiliza SQLite. As tabelas são criadas automaticamente na primeira execução:

- `vendas`: Dados principais das vendas
- `produtos`: Catálogo de produtos
- `lojas`: Informações das lojas
- `vendedores`: Dados dos vendedores
- `loja_vendedor`: Relacionamentos entre lojas e vendedores
- `usuarios`: Sistema de autenticação

### Logs

- **Batch logs:** `data/logs/batch.log` (rotativo, 5MB, 5 backups)
- **Web logs:** `data/logs/web.log` (rotativo, 2MB, 3 backups)
- **Duplicatas:** `data/reports/duplicates.csv` e `duplicates.log`

### Dropbox Integration

Para upload automático de relatórios:

1. Configure o token do Dropbox em `Token DropBox.docx`
2. Execute pipeline com `--enviar-dropbox`

## 📈 Pipeline de Processamento

1. **Extração:** Leitura do CSV de entrada
2. **Validação:** Verificação de integridade dos dados
3. **Correção:** Padronização e limpeza automática
4. **Inserção:** Armazenamento no banco de dados
5. **Relatórios:** Geração de PDFs e CSVs de qualidade
6. **Arquivamento:** Movimentação de arquivos processados

### Validações Implementadas

- CPF válido e único
- Datas em formato correto
- Valores numéricos positivos
- Campos obrigatórios preenchidos
- Relacionamentos consistentes (loja-vendedor)

## 📋 Formato dos Dados

### CSV de Entrada

Colunas obrigatórias:
- `id_cliente`, `nome_cliente`, `data_nascimento`, `rg`, `cpf`
- `endereco`, `numero`, `complemento`, `bairro`, `cidade`, `estado`, `cep`, `telefone`
- `codigo_produto`, `nome_produto`, `quantidade`, `valor_produto`
- `forma_pagamento`, `codigo_loja`, `nome_loja`, `codigo_vendedor`, `nome_vendedor`
- `data_venda`, `data_compra`, `status_venda`, `observacoes`

### Exemplo de Dados

```csv
id_cliente,nome_cliente,data_nascimento,rg,cpf,endereco,numero,complemento,bairro,cidade,estado,cep,telefone,codigo_produto,nome_produto,quantidade,valor_produto,forma_pagamento,codigo_loja,nome_loja,codigo_vendedor,nome_vendedor,data_venda,data_compra,status_venda,observacoes
1,João Silva,15/05/1985,12345678,12345678901,Rua A,123,,Centro,São Paulo,SP,01234567,11987654321,P001,Teclado Mecânico,2,150.00,Cartão,L01,Loja Centro,V001,Carlos Silva,20/10/2024,18/10/2024,CONCLUIDA,
```

## 🔄 Agendamento (Cron)

Exemplo de agendamento diário às 1h:

```bash
0 1 * * * cd /path/to/Vendas_teste && source venv_vendas/bin/activate && python main.py run >> data/logs/cron.log 2>&1
```

## 🧪 Testes

Execute os testes automatizados:

```bash
python -m pytest
```

## 📝 Logs e Monitoramento

- **Logs de processamento:** Acompanhe o progresso em `data/logs/batch.log`
- **Relatórios de qualidade:** PDFs gerados em `data/reports/`
- **Duplicatas detectadas:** Registradas em `data/reports/duplicates.csv`

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para detalhes.

## 🆘 Suporte

Para questões ou problemas:

1. Verifique os logs em `data/logs/`
2. Execute `python main.py dry-run` para validação
3. Abra uma issue no repositório

---

**Desenvolvido para análise de dados de vendas**




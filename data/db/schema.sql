-- =============================================
-- SCHEMA SQL PARA SISTEMA DE VENDAS
-- Compatível com SQLite - Usa apenas -- para comentários
-- =============================================

-- Configurações do SQLite para melhor performance
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA encoding = 'UTF-8';

-- =============================================
-- TABELA: vendedores
-- Armazena informações dos vendedores
-- =============================================
CREATE TABLE IF NOT EXISTS vendedores (
    codigo_vendedor TEXT PRIMARY KEY,
    nome_vendedor TEXT NOT NULL,
    email TEXT,
    telefone TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT 1
);

-- =============================================
-- TABELA: lojas  
-- Armazena informações das lojas
-- =============================================
CREATE TABLE IF NOT EXISTS lojas (
    codigo_loja TEXT PRIMARY KEY,
    nome_loja TEXT NOT NULL,
    endereco TEXT,
    cidade TEXT,
    estado TEXT,
    telefone TEXT,
    sellers_finalized BOOLEAN DEFAULT 0,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT 1
);

-- =============================================
-- TABELA: produtos
-- Armazena informações dos produtos
-- =============================================
CREATE TABLE IF NOT EXISTS produtos (
    codigo_produto TEXT PRIMARY KEY,
    nome_produto TEXT NOT NULL,
    valor_produto REAL NOT NULL CHECK(valor_produto >= 0),
    categoria TEXT,
    descricao TEXT,
    estoque_minimo INTEGER DEFAULT 0,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT 1
);

-- =============================================
-- TABELA: vendas
-- Tabela principal de vendas
-- =============================================
CREATE TABLE IF NOT EXISTS vendas (
    id_venda INTEGER PRIMARY KEY AUTOINCREMENT,
    id_cliente INTEGER NOT NULL,
    nome_cliente TEXT NOT NULL,
    data_nascimento TEXT,
    rg TEXT,
    cpf TEXT NOT NULL,
    endereco TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade TEXT,
    estado TEXT,
    cep TEXT,
    telefone TEXT,
    codigo_produto TEXT NOT NULL,
    nome_produto TEXT,
    quantidade INTEGER NOT NULL CHECK(quantidade > 0),
    valor_produto REAL NOT NULL CHECK(valor_produto >= 0),
    data_venda TEXT NOT NULL,
    data_compra TEXT NOT NULL,
    forma_pagamento TEXT,
    codigo_loja TEXT NOT NULL,
    nome_loja TEXT,
    codigo_vendedor TEXT NOT NULL,
    nome_vendedor TEXT,
    valor_total REAL GENERATED ALWAYS AS (valor_produto * quantidade) STORED,
    status_venda TEXT DEFAULT 'CONCLUIDA',
    observacoes TEXT,
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Chaves estrangeiras
    FOREIGN KEY(codigo_produto) REFERENCES produtos(codigo_produto) 
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja) 
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor) 
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- =============================================
-- TABELA: loja_vendedor
-- Relacionamento muitos-para-muitos entre lojas e vendedores
-- =============================================
CREATE TABLE IF NOT EXISTS loja_vendedor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_loja TEXT NOT NULL,
    codigo_vendedor TEXT NOT NULL,
    data_inicio TEXT DEFAULT CURRENT_DATE,
    data_fim TEXT,
    ativo BOOLEAN DEFAULT 1,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Restrições de unicidade e chaves estrangeiras
    UNIQUE(codigo_loja, codigo_vendedor, data_inicio),
    FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja) 
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- =============================================
-- TABELA: usuarios
-- Usuários do sistema para acesso ao dashboard
-- =============================================
CREATE TABLE IF NOT EXISTS usuarios (
    login TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('admin', 'user', 'manager', 'vendedor')),
    nome TEXT NOT NULL,
    email TEXT,
    loja TEXT NOT NULL,
    codigo_vendedor TEXT,
    permissions TEXT NOT NULL DEFAULT '{}',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultimo_login TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT 1,
    
    FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
        ON UPDATE CASCADE ON DELETE SET NULL
);

-- =============================================
-- TABELA: sistema_logs
-- Logs de atividades do sistema
-- =============================================
CREATE TABLE IF NOT EXISTS sistema_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL CHECK(tipo IN ('INFO', 'WARNING', 'ERROR', 'SUCCESS', 'DEBUG')),
    modulo TEXT NOT NULL,
    mensagem TEXT NOT NULL,
    usuario TEXT,
    ip_address TEXT,
    user_agent TEXT,
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TABELA: importacoes
-- Controle de arquivos importados
-- =============================================
CREATE TABLE IF NOT EXISTS importacoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_arquivo TEXT NOT NULL,
    total_registros INTEGER NOT NULL,
    registros_importados INTEGER NOT NULL,
    registros_com_erro INTEGER DEFAULT 0,
    usuario_importacao TEXT,
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'CONCLUIDA'
);

-- =============================================
-- TRIGGERS PARA INTEGRIDADE DOS DADOS
-- =============================================

-- Trigger: Impede inserção em loja_vendedor se loja está finalizada
CREATE TRIGGER IF NOT EXISTS trg_loja_vendedor_before_insert
BEFORE INSERT ON loja_vendedor
FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN (SELECT sellers_finalized FROM lojas WHERE codigo_loja = NEW.codigo_loja) = 1
        THEN RAISE(ABORT, 'Loja está finalizada; não é possível adicionar vendedores')
    END;
END;

-- Trigger: Impede exclusão de relação se loja está finalizada
CREATE TRIGGER IF NOT EXISTS trg_loja_vendedor_before_delete
BEFORE DELETE ON loja_vendedor
FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN (SELECT sellers_finalized FROM lojas WHERE codigo_loja = OLD.codigo_loja) = 1
        THEN RAISE(ABORT, 'Loja está finalizada; não é possível remover vendedores')
    END;
END;

-- Trigger: Garante que vendedor pertence à loja da venda
CREATE TRIGGER IF NOT EXISTS trg_vendas_vendedor_must_belong_to_loja
BEFORE INSERT ON vendas
FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM loja_vendedor lv
            WHERE lv.codigo_loja = NEW.codigo_loja
              AND lv.codigo_vendedor = NEW.codigo_vendedor
              AND lv.ativo = 1
        )
        THEN RAISE(ABORT, 'Vendedor não está atribuído à loja informada')
    END;
END;

-- Trigger: Atualiza data_atualizacao em vendas
CREATE TRIGGER IF NOT EXISTS trg_vendas_after_update
AFTER UPDATE ON vendas
FOR EACH ROW
BEGIN
    UPDATE vendas SET data_registro = CURRENT_TIMESTAMP WHERE id_venda = NEW.id_venda;
END;

-- Trigger: Log de novas vendas
CREATE TRIGGER IF NOT EXISTS trg_vendas_after_insert_log
AFTER INSERT ON vendas
FOR EACH ROW
BEGIN
    INSERT INTO sistema_logs (tipo, modulo, mensagem, usuario)
    VALUES ('INFO', 'VENDAS', 'Nova venda inserida - ID: ' || NEW.id_venda || ', Cliente: ' || NEW.nome_cliente, 'SISTEMA');
END;

-- Trigger: Validação de CPF único por data
CREATE TRIGGER IF NOT EXISTS trg_vendas_cpf_unique_per_date
BEFORE INSERT ON vendas
FOR EACH ROW
WHEN NEW.cpf IS NOT NULL AND NEW.cpf != ''
BEGIN
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM vendas 
            WHERE cpf = NEW.cpf 
            AND data_venda = NEW.data_venda
            AND id_venda != NEW.id_venda
        )
        THEN RAISE(ABORT, 'CPF duplicado para a mesma data de venda')
    END;
END;

-- =============================================
-- ÍNDICES PARA PERFORMANCE
-- =============================================

-- Índices para tabela vendas
CREATE INDEX IF NOT EXISTS idx_vendas_cpf ON vendas(cpf);
CREATE INDEX IF NOT EXISTS idx_vendas_data_venda ON vendas(data_venda);
CREATE INDEX IF NOT EXISTS idx_vendas_data_compra ON vendas(data_compra);
CREATE INDEX IF NOT EXISTS idx_vendas_loja ON vendas(codigo_loja);
CREATE INDEX IF NOT EXISTS idx_vendas_vendedor ON vendas(codigo_vendedor);
CREATE INDEX IF NOT EXISTS idx_vendas_produto ON vendas(codigo_produto);
CREATE INDEX IF NOT EXISTS idx_vendas_cliente ON vendas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_vendas_valor_total ON vendas(valor_total);
CREATE INDEX IF NOT EXISTS idx_vendas_forma_pagamento ON vendas(forma_pagamento);

-- Índices para tabela loja_vendedor
CREATE INDEX IF NOT EXISTS idx_loja_vendedor_loja ON loja_vendedor(codigo_loja);
CREATE INDEX IF NOT EXISTS idx_loja_vendedor_vendedor ON loja_vendedor(codigo_vendedor);
CREATE INDEX IF NOT EXISTS idx_loja_vendedor_ativo ON loja_vendedor(ativo);

-- Índices para tabela usuarios
CREATE INDEX IF NOT EXISTS idx_usuarios_login ON usuarios(login);
CREATE INDEX IF NOT EXISTS idx_usuarios_loja ON usuarios(loja);
CREATE INDEX IF NOT EXISTS idx_usuarios_role ON usuarios(role);
CREATE INDEX IF NOT EXISTS idx_usuarios_vendedor ON usuarios(codigo_vendedor);

-- Índices para tabela sistema_logs
CREATE INDEX IF NOT EXISTS idx_logs_data ON sistema_logs(data_registro);
CREATE INDEX IF NOT EXISTS idx_logs_tipo ON sistema_logs(tipo);
CREATE INDEX IF NOT EXISTS idx_logs_modulo ON sistema_logs(modulo);

-- Índices para tabela produtos
CREATE INDEX IF NOT EXISTS idx_produtos_categoria ON produtos(categoria);
CREATE INDEX IF NOT EXISTS idx_produtos_ativo ON produtos(ativo);

-- =============================================
-- VIEWS PARA RELATÓRIOS
-- =============================================

-- View: Vendas consolidadas com todas as informações
CREATE VIEW IF NOT EXISTS vw_vendas_consolidadas AS
SELECT 
    v.id_venda,
    v.id_cliente,
    v.nome_cliente,
    v.data_venda,
    v.data_compra,
    p.nome_produto,
    p.categoria as categoria_produto,
    v.valor_produto,
    v.quantidade,
    v.valor_total,
    l.nome_loja,
    vd.nome_vendedor,
    v.forma_pagamento,
    v.cidade,
    v.estado,
    v.status_venda,
    v.data_registro
FROM vendas v
JOIN produtos p ON v.codigo_produto = p.codigo_produto
JOIN lojas l ON v.codigo_loja = l.codigo_loja
JOIN vendedores vd ON v.codigo_vendedor = vd.codigo_vendedor
WHERE v.data_venda IS NOT NULL;

-- View: Performance de vendedores
CREATE VIEW IF NOT EXISTS vw_performance_vendedores AS
SELECT 
    vd.codigo_vendedor,
    vd.nome_vendedor,
    l.nome_loja,
    COUNT(v.id_venda) as total_vendas,
    SUM(v.valor_total) as valor_total_vendido,
    AVG(v.valor_total) as ticket_medio,
    MAX(v.data_venda) as ultima_venda
FROM vendedores vd
LEFT JOIN vendas v ON vd.codigo_vendedor = v.codigo_vendedor
LEFT JOIN lojas l ON v.codigo_loja = l.codigo_loja
WHERE v.data_venda IS NOT NULL
GROUP BY vd.codigo_vendedor, vd.nome_vendedor, l.nome_loja;

-- View: Performance de lojas
CREATE VIEW IF NOT EXISTS vw_performance_lojas AS
SELECT 
    l.codigo_loja,
    l.nome_loja,
    l.cidade,
    l.estado,
    COUNT(v.id_venda) as total_vendas,
    SUM(v.valor_total) as valor_total_vendido,
    COUNT(DISTINCT v.codigo_vendedor) as total_vendedores,
    COUNT(DISTINCT v.id_cliente) as total_clientes,
    AVG(v.valor_total) as ticket_medio
FROM lojas l
LEFT JOIN vendas v ON l.codigo_loja = v.codigo_loja
WHERE v.data_venda IS NOT NULL
GROUP BY l.codigo_loja, l.nome_loja, l.cidade, l.estado;

-- View: Produtos mais vendidos
CREATE VIEW IF NOT EXISTS vw_produtos_mais_vendidos AS
SELECT 
    p.codigo_produto,
    p.nome_produto,
    p.categoria,
    p.valor_produto,
    SUM(v.quantidade) as total_vendido,
    SUM(v.valor_total) as valor_total_vendido,
    COUNT(v.id_venda) as total_vendas
FROM produtos p
JOIN vendas v ON p.codigo_produto = v.codigo_produto
WHERE v.data_venda IS NOT NULL
GROUP BY p.codigo_produto, p.nome_produto, p.categoria, p.valor_produto
ORDER BY total_vendido DESC;

-- View: Vendas por período (mensal)
CREATE VIEW IF NOT EXISTS vw_vendas_mensais AS
SELECT 
    strftime('%Y-%m', data_venda) as mes_ano,
    COUNT(*) as total_vendas,
    SUM(valor_total) as valor_total,
    AVG(valor_total) as ticket_medio,
    COUNT(DISTINCT id_cliente) as clientes_unicos
FROM vendas
WHERE data_venda IS NOT NULL
GROUP BY strftime('%Y-%m', data_venda)
ORDER BY mes_ano DESC;

-- View: Resumo para dashboard
CREATE VIEW IF NOT EXISTS vw_dashboard_resumo AS
SELECT 
    (SELECT COUNT(*) FROM vendas) as total_vendas,
    (SELECT SUM(valor_total) FROM vendas) as valor_total_vendido,
    (SELECT COUNT(DISTINCT id_cliente) FROM vendas) as total_clientes,
    (SELECT COUNT(DISTINCT codigo_vendedor) FROM vendas) as total_vendedores,
    (SELECT COUNT(*) FROM lojas WHERE ativo = 1) as total_lojas_ativas,
    (SELECT AVG(valor_total) FROM vendas) as ticket_medio_geral;

-- =============================================
-- DADOS INICIAIS (INSERÇÕES BÁSICAS)
-- =============================================

-- Inserir produtos base
INSERT OR IGNORE INTO produtos (codigo_produto, nome_produto, valor_produto, categoria) VALUES
('P001', 'Notebook', 3500.00, 'Informática'),
('P002', 'Smartphone', 2500.00, 'Telefonia'),
('P003', 'Tablet', 1800.00, 'Informática'),
('P004', 'Monitor', 950.00, 'Informática'),
('P005', 'Teclado', 150.00, 'Informática'),
('P006', 'Caixa de Som', 200.00, 'Áudio'),
('P007', 'Mouse', 80.00, 'Informática'),
('P008', 'Impressora', 600.00, 'Informática');

-- Inserir lojas base
INSERT OR IGNORE INTO lojas (codigo_loja, nome_loja, cidade, estado, telefone) VALUES
('L001', 'Loja Centro', 'São Paulo', 'SP', '(11) 3333-4444'),
('L002', 'Loja Shopping', 'Rio de Janeiro', 'RJ', '(21) 2222-3333'),
('L003', 'Loja Bairro', 'Belo Horizonte', 'MG', '(31) 4444-5555');

-- Inserir vendedores base
INSERT OR IGNORE INTO vendedores (codigo_vendedor, nome_vendedor, email, telefone) VALUES
('V001', 'Carlos Silva', 'carlos.silva@empresa.com', '(11) 99999-1111'),
('V002', 'Maria Oliveira', 'maria.oliveira@empresa.com', '(11) 99999-2222'),
('V003', 'João Souza', 'joao.souza@empresa.com', '(21) 99999-3333'),
('V004', 'Antonio Santos', 'antonio.santos@empresa.com', '(21) 99999-4444'),
('V005', 'Barone Mendes', 'barone.mendes@empresa.com', '(31) 99999-5555'),
('V006', 'Thiago Costa', 'thiago.costa@empresa.com', '(31) 99999-6666'),
('V007', 'Mackenzie Nogueira', 'mackenzie.nogueira@empresa.com', '(21) 99999-7777');

-- Inserir relações loja-vendedor
INSERT OR IGNORE INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES
('L001', 'V001'),
('L001', 'V002'),
('L002', 'V003'),
('L002', 'V004'),
('L002', 'V007'),
('L003', 'V005'),
('L003', 'V006');

-- Inserir usuários do sistema
INSERT OR IGNORE INTO usuarios (login, password, role, nome, loja, permissions) VALUES
('admin', 'senha123', 'admin', 'Administrador do Sistema', 'Todas', '{
    "ver_filtros": true,
    "ver_indicadores": true,
    "ver_graficos": true,
    "executar_pipeline": true,
    "analisar_todas_lojas": true,
    "upload_csv": true,
    "configurar_usuarios": true,
    "acesso_total": true
}'),
('csilva', 'csilva1976', 'admin', 'Carlos Silva', 'Loja Centro', '{
    "ver_filtros": true,
    "ver_indicadores": true,
    "ver_graficos": true,
    "executar_pipeline": true,
    "analisar_todas_lojas": false,
    "upload_csv": true,
    "configurar_usuarios": false
}'),
('maoliveira', 'maoliveira1980', 'user', 'Maria Oliveira', 'Loja Centro', '{
    "ver_filtros": false,
    "ver_indicadores": true,
    "ver_graficos": true,
    "executar_pipeline": false,
    "analisar_todas_lojas": false,
    "upload_csv": false
}'),
('josouza', 'josouza1986', 'user', 'João Souza', 'Loja Shopping', '{
    "ver_filtros": false,
    "ver_indicadores": true,
    "ver_graficos": true,
    "executar_pipeline": false,
    "analisar_todas_lojas": false,
    "upload_csv": false
}'),
('baronem', 'baronem1990', 'user', 'Barone Mendes', 'Loja Bairro', '{
    "ver_filtros": false,
    "ver_indicadores": true,
    "ver_graficos": true,
    "executar_pipeline": false,
    "analisar_todas_lojas": false,
    "upload_csv": false
}');

-- Log inicial do sistema
INSERT OR IGNORE INTO sistema_logs (tipo, modulo, mensagem) VALUES
('INFO', 'SISTEMA', 'Banco de dados inicializado com sucesso - Schema aplicado'),
('INFO', 'SISTEMA', 'Dados iniciais carregados: produtos, lojas, vendedores e usuários');

-- =============================================
-- MENSAGEM FINAL
-- =============================================

-- Log de conclusão
INSERT OR IGNORE INTO sistema_logs (tipo, modulo, mensagem) VALUES
('SUCCESS', 'SCHEMA', 'Schema SQL executado com sucesso - Sistema pronto para uso');
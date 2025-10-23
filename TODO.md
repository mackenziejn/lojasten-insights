# Otimizações de Memória para Dashboard de Vendas

## Problemas Identificados
- Carregamento completo de DataFrames grandes na memória
- Loops ineficientes sobre DataFrames inteiros
- Múltiplas cópias de DataFrames
- Processamento síncrono de grandes volumes

## Plano de Correção

### 1. Otimizar Carregamento de Dados ✅
- [x] Implementar lazy loading no dashboard
- [x] Usar chunks para carregar dados do SQLite
- [x] Adicionar paginação para grandes datasets

### 2. Reduzir Uso de Memória em DataFrames ✅
- [x] Otimizar dtypes (usar categorias, int32 ao invés de int64)
- [x] Limpar DataFrames intermediários após uso
- [x] Usar inplace operations quando possível

### 3. Processar Dados em Chunks no Pipeline ✅
- [x] Modificar pipeline para processar em batches
- [x] Implementar streaming para inserção no banco
- [x] Reduzir cópias desnecessárias de DataFrames

### 4. Configurações do VSCode ✅
- [x] Configurar limites de memória no VSCode
- [x] Desabilitar extensões desnecessárias
- [x] Ajustar configurações do Python extension

## Status
- [x] Análise inicial concluída
- [x] Implementação das otimizações concluída

## Recomendações Adicionais
- Considere usar VSCode stable ao invés do Insiders para maior estabilidade
- Monitore o uso de memória com ferramentas como `htop` ou VSCode Memory Usage
- Para datasets muito grandes (>100k linhas), considere processamento em background
- Use virtual environment isolado para evitar conflitos

import re
from datetime import datetime

def corrigir_linha(row):
    """
    Corrige campos comuns com erros:
    - CPF: mantém apenas números, preenche com zeros se incompleto
    - Telefone: remove símbolos, preenche com zeros se incompleto
    - Datas: substitui inválidas por padrão ou data atual
    """
    # CPF: manter apenas números, preencher com zeros se necessário
    cpf = re.sub(r'\D', '', str(row.get('cpf', '')))
    row['cpf'] = cpf.zfill(11)[:11]

    # Telefone: remover símbolos, preencher com zeros se necessário
    telefone = re.sub(r'\D', '', str(row.get('telefone', '')))
    row['telefone'] = telefone.zfill(10)[:11]

    # Data de nascimento: tentar converter, senão usar padrão
    nascimento = row.get('data_nascimento', '')
    try:
        datetime.strptime(nascimento, "%d/%m/%Y")
    except (ValueError, TypeError):
        row['data_nascimento'] = "01/01/2000"

    # Data de compra: se inválida ou futura, usar data atual
    compra = row.get('data_compra', '')
    try:
        data_compra = datetime.strptime(compra, "%d/%m/%Y")
        if data_compra > datetime.now():
            row['data_compra'] = datetime.now().strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        row['data_compra'] = datetime.now().strftime("%d/%m/%Y")

    return row

def validar_linha(row):
    """
    Valida os campos principais após correção:
    - CPF: deve ter 11 dígitos numéricos
    - Telefone: deve ter entre 10 e 11 dígitos
    - Datas: devem estar em formato válido e coerente
    """
    erros = []

    cpf = str(row.get('cpf', ''))
    if not re.fullmatch(r'\d{11}', cpf):
        erros.append("CPF inválido")

    telefone = str(row.get('telefone', ''))
    if not re.fullmatch(r'\d{10,11}', telefone):
        erros.append("Telefone inválido")

    nascimento = row.get('data_nascimento', '')
    try:
        datetime.strptime(nascimento, "%d/%m/%Y")
    except (ValueError, TypeError):
        erros.append("Data de nascimento inválida")

    compra = row.get('data_compra', '')
    try:
        data_compra = datetime.strptime(compra, "%d/%m/%Y")
        if data_compra > datetime.now():
            erros.append("Data de compra futura")
    except (ValueError, TypeError):
        erros.append("Data de compra inválida")

    return erros

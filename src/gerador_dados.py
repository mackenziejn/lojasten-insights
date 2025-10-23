import pandas as pd
from faker import Faker
import random
import os

def gerar_dados_fake(caminho_csv="data/raw/vendas.csv", quantidade=100):
    os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)

    fake = Faker("pt_BR")
    Faker.seed(42)
    random.seed(42)

    produtos = [
        ("001", "Notebook", 3500.00),
        ("002", "Smartphone", 2200.00),
        ("003", "Tablet", 1800.00),
        ("004", "Monitor", 950.00),
        ("005", "Teclado", 150.00),
        ("006", "Caixa de som", 200.00)
    ]

    formas_pagamento = ["Cartão de Crédito", "Cartão de Débito", "Pix", "Dinheiro", "Boleto"]
    lojas = [("L001", "Loja Centro"), ("L002", "Loja Shopping"), ("L003", "Loja Bairro")]
    vendedores_dict = {
        "V001": "Carlos",
        "V002": "Fernanda",
        "V003": "Joao",
        "V004": "Mariana",
        "V005": "Barone",
        "V006": "Thiago",
        "V007": "Mackenzie"
    }
    loja_vendedor = {
        "L001": ["V001", "V002"],
        "L002": ["V003", "V004", "V007"],
        "L003": ["V005", "V006"]
    }

    dados = []
    cpfs_gerados = set()

    for i in range(1, quantidade + 1):
        id_cliente = i
        nome_cliente = fake.name()
        nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
        rg = fake.random_number(digits=7, fix_len=False)
        cpf = fake.cpf()
        while cpf in cpfs_gerados:
            cpf = fake.cpf()
        cpfs_gerados.add(cpf)
        endereco = fake.street_address()
        numero = fake.building_number()
        # some Faker providers for pt_BR may not expose these helpers; use safe fallbacks
        _secondary = getattr(fake, 'secondary_address', None)
        complemento = _secondary() if callable(_secondary) else ''
        _neigh = getattr(fake, 'neighborhood', None)
        bairro = _neigh() if callable(_neigh) else fake.city()
        cidade = fake.city()
        # use state_abbr when available
        _state_abbr = getattr(fake, 'state_abbr', None)
        estado = _state_abbr() if callable(_state_abbr) else fake.state()
        cep = fake.postcode()
        telefone = fake.phone_number()

        cod_prod, nome_prod, valor = random.choice(produtos)
        quantidade_prod = random.randint(1, 5)
        forma_pagamento = random.choice(formas_pagamento)
        cod_loja, nome_loja = random.choice(lojas)
        cod_vend = random.choice(loja_vendedor[cod_loja])
        nome_vend = vendedores_dict[cod_vend]

        # ✅ Gera a data da venda dentro do loop
        data_vend = fake.date_between(start_date='-1y', end_date='today').strftime("%d/%m/%Y")

        dados.append({
            "id_cliente": id_cliente,
            "nome_cliente": nome_cliente,
            "data_nascimento": nascimento,
            "rg": rg,
            "cpf": cpf,
            "endereco": endereco,
            "numero": numero,
            "complemento": complemento,
            "bairro": bairro,
            "cidade": cidade,
            "estado": estado,
            "cep": cep,
            "telefone": telefone,
            "codigo_produto": cod_prod,
            "nome_produto": nome_prod,
            "quantidade": quantidade_prod,
            "valor_produto": valor,
            "data_venda": data_vend,
            "forma_pagamento": forma_pagamento,
            "codigo_loja": cod_loja,
            "nome_loja": nome_loja,
            "codigo_vendedor": cod_vend,
            "nome_vendedor": nome_vend
        })

    df = pd.DataFrame(dados)
    df.to_csv(caminho_csv, index=False, sep=";")
    print(f"✅ Arquivo vendas.csv gerado com sucesso em {caminho_csv}")

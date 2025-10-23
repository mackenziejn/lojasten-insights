import pandas as pd
import re
from unidecode import unidecode

def clean_text(text):
    if pd.isna(text):
        return text
    # Remove accents and cedillas
    text = unidecode(text)
    # Remove titles
    text = re.sub(r'\b(Sr\.?|Sra\.?|Dr\.?|Dra\.?)\s+', '', text, flags=re.IGNORECASE)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_phone(phone):
    if pd.isna(phone):
        return phone
    # Remove +55, spaces, parentheses, dashes, dots
    phone = re.sub(r'\+55\s*|\s|\(|\)|\-|\.', '', phone)
    return phone

def extract_numero_from_endereco(endereco):
    if pd.isna(endereco):
        return '0000'
    # Find the last number in the address
    match = re.search(r'(\d+)(?:\D*$)', endereco)
    if match:
        num = match.group(1)
        # Take only last 4 digits, pad with zeros if less
        num = num[-4:].zfill(4)
        return num
    return '0000'

def clean_vendas_csv(input_path, output_path):
    df = pd.read_csv(input_path)

    # Columns to clean text
    text_columns = ['nome_cliente', 'endereco', 'bairro', 'cidade', 'nome_loja', 'nome_vendedor']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # Extract numero from endereco and set to 4 digits
    if 'endereco' in df.columns:
        df['numero'] = df['endereco'].apply(extract_numero_from_endereco)
        # Remove the number and comma from endereco
        df['endereco'] = df['endereco'].str.replace(r',\s*\d+$', '', regex=True)

    # Clean telefone
    if 'telefone' in df.columns:
        df['telefone'] = df['telefone'].apply(clean_phone)

    # Standardize cidade and estado
    if 'cidade' in df.columns:
        df['cidade'] = 'Sao Paulo'
    if 'estado' in df.columns:
        df['estado'] = 'SP'

    # Clean CPF: remove dots and dashes
    if 'cpf' in df.columns:
        df['cpf'] = df['cpf'].str.replace(r'\.|\-', '', regex=True)

    # Clean RG: remove dots and dashes
    if 'rg' in df.columns:
        df['rg'] = df['rg'].str.replace(r'\.|\-', '', regex=True)

    # Clean CEP: remove dashes
    if 'cep' in df.columns:
        df['cep'] = df['cep'].astype(str).str.replace(r'\-', '', regex=True)

    # Save cleaned CSV
    df.to_csv(output_path, index=False)
    print(f"Cleaned CSV saved to {output_path}")

if __name__ == "__main__":
    clean_vendas_csv('data/raw/vendas.csv', 'data/raw/vendas_clean.csv')

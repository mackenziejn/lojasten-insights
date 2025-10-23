import pandas as pd
import unidecode
import re
import os

def clean_vendas_csv(input_path='data/raw/vendas.csv', output_path='data/raw/vendas.csv'):
    """
    Cleans the vendas.csv file:
    - Removes accents, cedillas, and special characters using unidecode.
    - Removes titles like Dr., Dra., Sr., Sra. from nome_cliente.
    - Applies cleaning to relevant fields: nome_cliente, endereco, nome_vendedor, bairro, cidade.
    """
    # Ensure directories exist
    os.makedirs(os.path.dirname(input_path), exist_ok=True)

    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return

    # Load CSV
    df = pd.read_csv(input_path)

    # Fields to clean for accents/special chars
    fields_to_unidecode = ['nome_cliente', 'endereco', 'nome_vendedor', 'bairro', 'cidade', 'complemento']

    for field in fields_to_unidecode:
        if field in df.columns:
            df[field] = df[field].astype(str).apply(lambda x: unidecode.unidecode(str(x)))

    # Remove titles from nome_cliente
    if 'nome_cliente' in df.columns:
        titles_pattern = r'^(Dr\.|Dra\.|Sr\.|Sra\.|Prof\.|Profª\.)\s*'
        df['nome_cliente'] = df['nome_cliente'].astype(str).apply(lambda x: re.sub(titles_pattern, '', x).strip())

    # Save cleaned CSV (overwrite)
    df.to_csv(output_path, index=False)
    print(f"Cleaned CSV saved to {output_path}. Rows: {len(df)}")

if __name__ == "__main__":
    clean_vendas_csv()

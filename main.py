import os
import pandas as pd
from typing import List, Dict, Union, Optional
import os
from dotenv import load_dotenv

#obtem o arquivo ou a lista de arquivos a serem convertidos
def get_files(input_path):
    if os.path.isdir(input_path):
        return [
            os.path.join(input_path, f)
            for f in os.listdir(input_path)
            if f.lower().endswith(('.xls', '.xlsx'))
        ]
    elif os.path.isfile(input_path) and input_path.lower().endswith(('.xls', '.xlsx')):
        return [input_path]
    else:
        raise FileNotFoundError(f"Diretório ou arquivo inválido: {input_path}")

#le o arquivo excel
def read_file(file_path):
    return pd.read_excel(file_path)

#tipa as colunas caso tenha sido fornecido um schema
def types(df: pd.DataFrame, schema: Optional[Dict[str, Union[str, type]]] = None):
    if schema:
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    print(f"Erro ao converter coluna '{col}' para {dtype}: {e}")
    return df

#funcao principal que converte para parquet
def to_parquet_file(input_path: str, output_path: str, schema: Optional[Dict[str, Union[str, type]]] = None):
    excel_files = get_files(input_path)
    dataframes = []

    for file in excel_files:
        df = read_file(file)
        df = types(df, schema)
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_parquet(output_path, index=False)
    print(f"Parquet file salvo em: {output_path}")

# Caminhos para ler e salvar os arquivos
load_dotenv()  # aqui estou usando as variaveis de ambiente para nao informar direto no codigo

input_path = os.getenv("IMPUT_PATH")  
output_path = os.getenv("OUTPUT_PATH") 

# Schema colunas: {'coluna1': 'int', 'coluna2': 'float', 'coluna3': 'str'}
schema = {
    'id': 'int64',
    'valor': 'float',
    'descricao': 'string'
}

if __name__ == "__main__":
    to_parquet_file(input_path, output_path, schema)

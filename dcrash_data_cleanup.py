import pandas as pd
import psycopg2
import uuid
import os
import re

#Configuração do banco PostgreSQL
banco_config = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'PUC_TECH_DASHBOARD',
    'user': 'postgres',
    'password': 'chiko'
}

table_name = 'Acidentes'

#Mapeamento entre CSV e colunas do PostgreSQL
cols_mapping = {
    'NOME_CONC': 'concessionaria',
    'RODOVIA': 'rodovia',
    'MARCO_QM': 'km',
    'SENTIDO': 'sentido',
    'CLASS_ACID': 'classificação_acidente',
    'TIPO_ACID': 'Tipo_acidente',
    'CAUSA': 'causa',
    'METEORO': 'meteoro',
    'VISIB': 'visibilidade',
    'VEIC': 'veiculo',
    'TIPO_PISTA': 'pista',
    'LATITUDE': 'latitude',
    'LONGITUDE': 'longitude'
}

def carregar_csv(local_path="acidentes_2025.csv"):
    if not os.path.exists(local_path):
        print(f"[ERRO] Arquivo '{local_path}' não encontrado.")
        return None

    try:
        return pd.read_csv(local_path, sep=",", low_memory=False, on_bad_lines="skip")
    except Exception as e:
        print(f"[ERRO] Falha ao ler CSV: {e}")
        return None


def limpar_preparar(df):
    #Combina data + hora
    df['data'] = pd.to_datetime(df['DATA'] + ' ' + df['HR_ACID'], errors='coerce')

    #Filtra apenas anos desejados(mude caso necessário)
    anos_desejados = [2021, 2022, 2023, 2024, 2025]
    df = df[df['data'].dt.year.isin(anos_desejados)].copy()

    #Converte MARCO_QM para número e depois para inteiro (remove casas decimais)
    df['km'] = pd.to_numeric(
        df['MARCO_QM'].astype(str).str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0).astype(int)

    #Conversões seguras
    df['fatalidades'] = pd.to_numeric(df['QTD_VIT_FATAL'], errors='coerce').fillna(0).astype(int)
    df['latitude'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

    #Regex para detectar valores inválidos
    regex_invalido = re.compile(
        r'SEM\s+INFO(?:RMAÇÃO)?|NULO|NÃO\s+INFORMADO|^0$|NULL|SEM\s+INFO/NULO/0',
        flags=re.IGNORECASE
    )

    #Aplica limpeza nas colunas de texto
    for csv_col in cols_mapping.keys():
        if csv_col in df.columns:
            df[csv_col] = df[csv_col].astype(str).str.strip()
            df = df[~df[csv_col].str.contains(regex_invalido, na=True)].copy()

    #Remove registros com dados essenciais ausentes
    campos_essenciais = list(cols_mapping.keys()) + ['data', 'km', 'fatalidades', 'latitude', 'longitude']
    df = df.dropna(subset=campos_essenciais)

    print(f"[INFO] Registros após limpeza: {len(df)}")
    return df



def inserir_batch(df, table, cfg):
    if df.empty:
        print("[ERRO] Nenhum dado para inserir.")
        return

    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    cols = ['ID'] + list(cols_mapping.values()) + ['data', 'fatalidades']
    placeholders = ','.join(['%s'] * len(cols))
    col_names = ', '.join([f'"{col}"' for col in cols])
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

    records = []
    for _, row in df.iterrows():
        try:
            rec = [
                str(uuid.uuid4()) # usa o _id do CSV
            ] + [row[k] for k in cols_mapping.keys()] + [
                row['data'], row['fatalidades']
            ]
            records.append(rec)
        except Exception as e:
            print(f"[AVISO] Linha ignorada por erro: {e}")
            continue

    if not records:
        print("[ERRO] Nenhum registro válido foi montado para inserção.")
        return

    print(f"[INFO] Inserindo {len(records)} registros na tabela '{table}'...")

    try:
        cur.executemany(sql, records)
        conn.commit()
        print("[SUCESSO] Dados inseridos com sucesso!")
    except Exception as e:
        print(f"[ERRO] Falha na inserção: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def main():
    df = carregar_csv("acidentes_2025.csv")
    if df is None or df.empty:
        print("[ERRO] CSV não carregado ou vazio.")
        return

    print(f"[INFO] Linhas carregadas: {len(df)}")

    df_clean = limpar_preparar(df)
    if df_clean is None or df_clean.empty:
        print("[ERRO] DataFrame limpo está vazio ou inválido.")
        return

    print(f"[INFO] Linhas após limpeza: {len(df_clean)}")

    inserir_batch(df_clean, table_name, banco_config)


if __name__ == "__main__":
    main()

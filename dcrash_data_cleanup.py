import pandas as pd
import psycopg2
import uuid
import os

# Configuração do banco PostgreSQL
banco_config = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'PUC_TECH_DASHBOARD',
    'user': 'postgres',
    'password': 'twitter#2025'
}

table_name = 'Acidentes'

# Mapeamento entre CSV e colunas do PostgreSQL
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
    # Combinar data + hora
    df.loc[:, 'data'] = pd.to_datetime(df['DATA'] + ' ' + df['HR_ACID'], errors='coerce')

    # Filtrar apenas anos desejados
    anos_desejados = [2024, 2025]
    df = df[df['data'].dt.year.isin(anos_desejados)].copy()

    # Converter MARCO_QM para número (com separador decimal tratado) e armazenar em 'km'
    df['km'] = pd.to_numeric(df['MARCO_QM'].astype(str).str.replace(',', '.', regex=False), errors='coerce')

    # Conversões seguras
    df['fatalidades'] = pd.to_numeric(df['QTD_VIT_FATAL'], errors='coerce').fillna(0).astype(int)
    df['latitude'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

    # Lista de colunas para verificar e remover valores inválidos
    texto_colunas = [
        'NOME_CONC', 'RODOVIA', 'SENTIDO', 'CLASS_ACID',
        'TIPO_ACID', 'CAUSA', 'METEORO', 'VISIB', 'VEIC', 'TIPO_PISTA'
    ]

    valores_invalidos = ['SEM INFO', 'NULO', '0', 0]

    for col in texto_colunas:
        df = df[~df[col].astype(str).str.upper().isin(valores_invalidos)].copy()

    # Remover linhas com dados essenciais ausentes
    campos_essenciais = list(cols_mapping.keys()) + ['data', 'km', 'fatalidades', 'latitude', 'longitude']
    df = df.dropna(subset=campos_essenciais)

    return df


def inserir_batch(df, table, cfg):
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    cols = ['ID'] + list(cols_mapping.values()) + ['data', 'fatalidades']
    placeholders = ','.join(['%s'] * len(cols))

    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"

    records = []
    for _, row in df.iterrows():
        try:
            rec = [
                str(uuid.UUID(row['_id']))  # usa o _id do CSV
            ] + [row[k] for k in cols_mapping.keys()] + [
                row['data'], row['fatalidades']
            ]
            records.append(rec)
        except Exception:
            continue  # pula linhas com ID inválido

    cur.executemany(sql, records)
    conn.commit()
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

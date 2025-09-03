import pandas as pd
import psycopg2
import uuid

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
    'CLASS_ACID': 'classificacao_acidente',
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
    # lê com separador correto
    return pd.read_csv(local_path, sep=",", low_memory=False, on_bad_lines="skip")


def limpar_preparar(df):
    def limpar_preparar(df):
        # Combinar data + hora
        df.loc[:, 'data'] = pd.to_datetime(df['DATA'] + ' ' + df['HR_ACID'], errors='coerce')

        # Filtrar anos
        anos_desejados = [2024, 2025]
        df = df[df['data'].dt.year.isin(anos_desejados)].copy()

        # Converter MARCO_QM para número sem sobrescrever a coluna original
        df['km'] = pd.to_numeric(df['MARCO_QM'].astype(str).str.replace(',', '.'), errors='coerce')

        # Conversões
        df.loc[:, 'MARCO_QM'] = df['MARCO_QM'].astype(str).str.replace(',', '.')
        df.loc[:, 'km'] = pd.to_numeric(df['MARCO_QM'], errors='coerce')
        df.loc[:, 'fatalidades'] = pd.to_numeric(df['QTD_VIT_FATAL'], errors='coerce').fillna(0).astype(int)
        df.loc[:, 'latitude'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
        df.loc[:, 'longitude'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

        # Lista de colunas para verificar conteúdo inválido
        texto_colunas = [
            'NOME_CONC', 'RODOVIA', 'SENTIDO', 'CLASS_ACID',
            'TIPO_ACID', 'CAUSA', 'METEORO', 'VISIB', 'VEIC', 'TIPO_PISTA'
        ]

        valores_invalidos = ['SEM INFO', 'NULO', '0', 0]

        for col in texto_colunas:
            df = df[~df[col].astype(str).str.upper().isin(valores_invalidos)].copy()

        # Remove linhas com campos essenciais faltando
        campos_essenciais = list(cols_mapping.keys()) + ['data', 'km', 'fatalidades', 'latitude', 'longitude']
        df = df.dropna(subset=campos_essenciais)

        return df


def inserir_batch(df, table, cfg):
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    cols = ['id'] + list(cols_mapping.values()) + ['data', 'fatalidades']
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
    df = carregar_csv("acidentes_2025.csv")  # arquivo já baixado
    df_clean = limpar_preparar(df)
    inserir_batch(df_clean, table_name, banco_config)


if __name__ == "__main__":
    main()

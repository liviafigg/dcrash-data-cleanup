import pandas as pd
import psycopg2
import uuid
import os
import re
import glob  #Para buscar arquivos por padrão

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
    'CAUSA': 'causa',  #Pode não existir em todos os anos
    'METEORO': 'meteoro',
    'VISIB': 'visibilidade',
    'VEIC': 'veiculo',
    'TIPO_PISTA': 'pista',
    'LATITUDE': 'latitude',
    'LONGITUDE': 'longitude'
}

#Variações possíveis de nomes de colunas entre diferentes arquivos
col_variations = {
    "latitude": ["LATITUDE", "LAT", "LATIT", "LATITUD"],
    "longitude": ["LONGITUDE", "LONG", "LON", "LONGIT"],
    "veiculo": ["VEIC", "VEÍC", "VEÍCULOS", "QTD_VEIC"],
    "datahora": ["DTHR_OC", "DATA_HORA", "DATA_OCORRENCIA"],
    "concessionaria": ["NOME_CONC", "CONCESSIONÁRIA", "CONC"],
}


def detectar_coluna(df, nomes_possiveis):
    for nome in nomes_possiveis:
        if nome in df.columns:
            return nome
    return None


def carregar_csv(local_path):
    if not os.path.exists(local_path):
        print(f"[ERRO] Arquivo '{local_path}' não encontrado.")
        return None

    try:
        #Detecta separador verificando a primeira linha
        with open(local_path, 'r', encoding='utf-8') as f:
            primeira_linha = f.readline()
            sep = ";" if ";" in primeira_linha else ","

        #Carrega CSV com separador adequado
        df = pd.read_csv(local_path, sep=sep, low_memory=False, on_bad_lines="skip")

        #Detecta se existe coluna unificada de data/hora ou separada em DATA + HR_ACID
        if "DATA" in df.columns and "HR_ACID" in df.columns:
            #Caso igual ao CSV de 2025 (duas colunas separadas)
            df["data"] = pd.to_datetime(
                df["DATA"].astype(str).str.strip() + " " + df["HR_ACID"].astype(str).str.strip(),
                errors="coerce",
                dayfirst=True  #Garante leitura correta de datas em formato brasileiro (dd/mm/yyyy)
            )
        elif any(col.upper().startswith("DATA") for col in df.columns):
            #Caso igual aos CSVs de 2021–2024 (coluna única com data+hora)
            col_data = [c for c in df.columns if "DATA" in c.upper()][0]
            df["data"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
        else:
            col_data = detectar_coluna(df, col_variations["datahora"])
            if col_data:
                df["data"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
            else:
                print(f"[AVISO] Não foram encontradas colunas de data em {local_path}")

        return df

    except Exception as e:
        print(f"[ERRO] Falha ao ler CSV {local_path}: {e}")
        return None


def limpar_preparar(df):

    #Debug: mostra quantos registros válidos por ano existem antes da limpeza
    print("[DEBUG] Quantidade de datas válidas por ano (antes da limpeza):")
    print(df['data'].dt.year.value_counts(dropna=True).sort_index())

    #Limpeza da coluna km (mantendo casas decimais)
    if "MARCO_QM" in df.columns:
        df['km'] = pd.to_numeric(
            df['MARCO_QM'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        ).fillna(0)
    else:
        df['km'] = 0  #Fallback caso não exista MARCO_QM

    #Conversões seguras de colunas numéricas
    if "QTD_VIT_FATAL" in df.columns:
        df['fatalidades'] = pd.to_numeric(df['QTD_VIT_FATAL'], errors='coerce').fillna(0).astype(int)
    else:
        df['fatalidades'] = 0

    #Latitude e longitude (busca nomes diferentes)
    col_lat = detectar_coluna(df, col_variations["latitude"])
    col_lon = detectar_coluna(df, col_variations["longitude"])
    df['latitude'] = pd.to_numeric(df[col_lat], errors='coerce') if col_lat else None
    df['longitude'] = pd.to_numeric(df[col_lon], errors='coerce') if col_lon else None

    #Regex para valores inválidos (ex: SEM INFORMAÇÃO, NULO, etc.)
    regex_invalido = re.compile(
        r'SEM\s+INFO(?:RMAÇÃO)?|NULO|NÃO\s+INFORMADO|^0$|NULL|SEM\s+INFO/NULO/0',
        flags=re.IGNORECASE
    )

    #Aplica limpeza em todas as colunas de texto do mapeamento que existirem
    for csv_col in cols_mapping.keys():
        if csv_col in df.columns:
            df[csv_col] = df[csv_col].astype(str).str.strip()
            df = df[~df[csv_col].str.contains(regex_invalido, na=True)].copy()

    #Remove registros apenas se faltar data
    campos_essenciais = ['data']
    df = df.dropna(subset=campos_essenciais)

    print(f"[INFO] Registros após limpeza: {len(df)}")
    print("[INFO] Registros por ano após limpeza:")
    print(df['data'].dt.year.value_counts().sort_index())

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
                str(uuid.uuid4())  #Gera ID único
            ]
            for k in cols_mapping.keys():
                rec.append(row[k] if k in row else None)  #Colunas ausentes vão como None
            rec.extend([row['data'], row['fatalidades']])
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
    #Busca todos os arquivos que começam com acidentes_ e terminam em .csv
    arquivos = glob.glob("acidentes_*.csv")

    if not arquivos:
        print("[ERRO] Nenhum arquivo CSV encontrado com padrão 'acidentes_*.csv'.")
        return

    dfs = []
    for arquivo in arquivos:
        df = carregar_csv(arquivo)
        if df is not None and not df.empty:
            print(f"[INFO] Arquivo {arquivo} carregado com {len(df)} linhas.")
            dfs.append(df)
        else:
            print(f"[AVISO] Arquivo {arquivo} não carregado ou vazio.")

    if not dfs:
        print("[ERRO] Nenhum CSV válido encontrado.")
        return

    #Junta todos os DataFrames carregados
    df = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Total de linhas carregadas (todos arquivos): {len(df)}")

    #Limpeza e preparação
    df_clean = limpar_preparar(df)
    if df_clean is None or df_clean.empty:
        print("[ERRO] DataFrame limpo está vazio ou inválido.")
        return

    print(f"[INFO] Linhas após limpeza: {len(df_clean)}")

    #Inserção no banco
    inserir_batch(df_clean, table_name, banco_config)


if __name__ == "__main__":
    main()

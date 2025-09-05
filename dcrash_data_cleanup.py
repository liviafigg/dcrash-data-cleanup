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
    'CAUSA': 'causa',
    'METEORO': 'meteoro',
    'VISIB': 'visibilidade',
    'VEIC': 'veiculo',
    'TIPO_PISTA': 'pista',
    'LATITUDE': 'latitude',
    'LONGITUDE': 'longitude'
}

def carregar_csv(local_path):
    if not os.path.exists(local_path):
        print(f"[ERRO] Arquivo '{local_path}' não encontrado.")
        return None

    try:
        return pd.read_csv(local_path, sep=",", low_memory=False, on_bad_lines="skip")
    except Exception as e:
        print(f"[ERRO] Falha ao ler CSV {local_path}: {e}")
        return None


def limpar_preparar(df):
    #Combina data + hora
    df['data'] = pd.to_datetime(df['DATA'] + ' ' + df['HR_ACID'], errors='coerce')

    #Se a data não existir, usa o ano do nome do arquivo como fallback
    if "ano_origem" in df.columns:
        df.loc[df['data'].isna(), 'data'] = pd.to_datetime(
            df['ano_origem'].astype(str) + "-01-01", errors="coerce"
        )

    #Limpeza da coluna km
    df['km'] = pd.to_numeric(
        df['MARCO_QM'].astype(str).str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0)

    #Conversões seguras
    df['fatalidades'] = pd.to_numeric(df['QTD_VIT_FATAL'], errors='coerce').fillna(0).astype(int)
    df['latitude'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

    #Regex para valores inválidos
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
                str(uuid.uuid4())  #gera um ID único
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
    #Busca todos os arquivos que começam com acidentes_ e terminam em .csv
    arquivos = glob.glob("acidentes_*.csv")

    if not arquivos:
        print("[ERRO] Nenhum arquivo CSV encontrado com padrão 'acidentes_*.csv'.")
        return

    dfs = []
    for arquivo in arquivos:
        df = carregar_csv(arquivo)
        if df is not None and not df.empty:
            #Tenta extrair ano do nome do arquivo (para fallback)
            ano = re.findall(r'(\d{4})', arquivo)
            if ano:
                df["ano_origem"] = int(ano[0])
            dfs.append(df)
        else:
            print(f"[AVISO] Arquivo {arquivo} não carregado ou vazio.")

    if not dfs:
        print("[ERRO] Nenhum CSV válido encontrado.")
        return

    #Junta tudo
    df = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Total de linhas carregadas (todos arquivos): {len(df)}")

    #Limpeza e preparação
    df_clean = limpar_preparar(df)
    if df_clean is None or df_clean.empty:
        print("[ERRO] DataFrame limpo está vazio ou inválido.")
        return

    print(f"[INFO] Linhas após limpeza: {len(df_clean)}")

    #Resumo por ano
    if not df_clean.empty:
        df_clean['ano_final'] = df_clean['data'].dt.year
        resumo = df_clean.groupby('ano_final').size()
        print("\n[INFO] Registros por ano após limpeza:")
        for ano, qtd in resumo.items():
            print(f"   {ano}: {qtd} registros")

    #Insere no banco
    inserir_batch(df_clean, table_name, banco_config)


if __name__ == "__main__":
    main()

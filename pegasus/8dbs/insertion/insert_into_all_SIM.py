############################################################################################################################################################################
#  SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM #
############################################################################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from .data_wrangling import prepare_SIM

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SIM_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_SIM" do package "data_wrangling"

    df_TIPOBITO = prepare_SIM.get_TIPOBITO_treated()
    df_TIPOBITO.to_sql('tipobito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NAT1212 = prepare_SIM.get_NAT1212_treated(path)
    df_NAT1212.to_sql('naturale', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = prepare_SIM.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = prepare_SIM.get_CADMUN_treated()
    df_CADMUN.to_sql('codmunnatu', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA = prepare_SIM.get_RACA_treated()
    df_RACA.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESTCIV = prepare_SIM.get_ESTCIV_treated()
    df_ESTCIV.to_sql('estciv', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESC = prepare_SIM.get_INSTRUC_treated()
    df_ESC.to_sql('esc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCSERIE = prepare_SIM.get_ESCSERIE_treated()
    df_ESCSERIE.to_sql('esc2010', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABOCUP_2TCC = prepare_SIM.get_TABOCUP_2TCC_treated(path)
    df_TABOCUP_2TCC.to_sql('ocup', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnatu"
    df_CADMUN.to_sql('codmunres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LOCOCOR = prepare_SIM.get_LOCOCOR_treated()
    df_LOCOCOR.to_sql('lococor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNESDO18_3TCC = prepare_SIM.get_CNESDO18_3TCC_treated(path)
    df_CNESDO18_3TCC.to_sql('codestab', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnatu"
    df_CADMUN.to_sql('codmunocor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPMORTEOCO = prepare_SIM.get_TPMORTEOCO_treated(path)
    df_TPMORTEOCO.to_sql('tpmorteoco', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = prepare_SIM.get_CID10_treated(path)
    df_CID10.to_sql('causabas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TIPOVIOL = prepare_SIM.get_TIPOVIOL_treated()
    df_TIPOVIOL.to_sql('circobito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FONTINFO = prepare_SIM.get_FONTINFO_treated()
    df_FONTINFO.to_sql('fonte', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "causabas"
    df_CID10.to_sql('causabas_o', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ATESTANT = prepare_SIM.get_ATESTANT_treated()
    df_ATESTANT.to_sql('atestante', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FONTEINV = prepare_SIM.get_FONTEINV_treated()
    df_FONTEINV.to_sql('fonteinv', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCAGR1 = prepare_SIM.get_ESCAGR1_treated()
    df_ESCAGR1.to_sql('escmaeagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "escmaearg1"
    df_ESCAGR1.to_sql('escfalagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPOBITOCOR = prepare_SIM.get_TPOBITOCOR_treated(path)
    df_TPOBITOCOR.to_sql('tpobitocor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################################################################
#         copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas         #
###########################################################################################################################################################################
###########################################################################################################################################################################
#  MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE #
###########################################################################################################################################################################

# Função que utiliza "copy_expert" para a inserção de dados principais e "pandas.to_sql" para a inserção
# dos respectivos metadados no banco de dados "child_db"
def insert_into_main_table_and_arquivos(file_name, directory, date_ftp, device, child_db, connection_data):
    start = time.time()
    counting_rows = pd.read_sql(f'''SELECT COUNT('NOME') FROM {child_db}.arquivos''', con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {connection_data[0]}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do SIM
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:8]
    main_table = base.lower() + 'br'
    counting_rows = pd.read_sql(f'''SELECT COUNT(*) from {child_db}.{main_table}''', con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo DO{state}{year}...')

    # Chama a função "get_DOXXaaaa_treated" do módulo "prepare_SIM" do package "data_wrangling"
    df = prepare_SIM.get_DOXXaaaa_treated(state, year)
    # Inserção das colunas UF_DO e ANO_DO no objeto pandas DataFrame "df"
    df.insert(1, 'UF_DO', [state]*df.shape[0])
    df.insert(2, 'ANO_DO', [int(year)]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do SIM armazenado no objeto
    # pandas DataFrame "df"
    df.to_csv(base + state + year + '.csv', sep=',', header=False, index=False, encoding='iso-8859-1')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do SIM_XX
    f = open(base + state + year + '.csv', 'r')
    # Conecta ao banco de dados mãe "connection_data[0]" do SGBD PostgreSQL usando o módulo python "psycopg2"
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])
    # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
    cursor = conn.cursor()
    # Faz a inserção dos dados armazenados em "f" na tabela "main_table" do banco de dados "child_db"
    # usando o método "copy_expert" do "psycopg2"
    cursor.copy_expert(f'''COPY {child_db}.{main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()
    # Encerra o file handler
    f.close()
    # Remoção do arquivo "csv"
    os.remove(base + state + year  + '.csv')
    print(f'Terminou de inserir os dados do arquivo DO{state}{year} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo DO{state}{year} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

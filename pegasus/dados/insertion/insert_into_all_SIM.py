###########################################################################################################################
#   SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM   #
###########################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from transform.prepare_SIM import DataSimMain, DataSimAuxiliary

###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SIM_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataSimAuxiliary" do módulo "prepare_SIM" do package "data_wrangling"
    data_sim_auxiliary = DataSimAuxiliary(path)

    # Chama métodos da classe "DataSimAuxiliary" do módulo "prepare_SIM" do banco de dados SIM
    df_TIPOBITO = data_sim_auxiliary.get_TIPOBITO_treated()
    df_TIPOBITO.to_sql('tipobito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NAT1212 = data_sim_auxiliary.get_NAT1212_treated()
    df_NAT1212.to_sql('naturale', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = data_sim_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_sim_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_sim_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('codmunnatu', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA = data_sim_auxiliary.get_RACA_treated()
    df_RACA.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESTCIV = data_sim_auxiliary.get_ESTCIV_treated()
    df_ESTCIV.to_sql('estciv', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESC = data_sim_auxiliary.get_INSTRUC_treated()
    df_ESC.to_sql('esc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCSERIE = data_sim_auxiliary.get_ESCSERIE_treated()
    df_ESCSERIE.to_sql('esc2010', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABOCUP_2TCC = data_sim_auxiliary.get_TABOCUP_2TCC_treated()
    df_TABOCUP_2TCC.to_sql('ocup', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnatu"
    df_CADMUN.to_sql('codmunres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LOCOCOR = data_sim_auxiliary.get_LOCOCOR_treated()
    df_LOCOCOR.to_sql('lococor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNESDO18_3TCC = data_sim_auxiliary.get_CNESDO18_3TCC_treated()
    df_CNESDO18_3TCC.to_sql('codestab', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnatu"
    df_CADMUN.to_sql('codmunocor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPMORTEOCO = data_sim_auxiliary.get_TPMORTEOCO_treated()
    df_TPMORTEOCO.to_sql('tpmorteoco', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = data_sim_auxiliary.get_CID10_treated()
    df_CID10.to_sql('causabas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TIPOVIOL = data_sim_auxiliary.get_TIPOVIOL_treated()
    df_TIPOVIOL.to_sql('circobito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FONTINFO = data_sim_auxiliary.get_FONTINFO_treated()
    df_FONTINFO.to_sql('fonte', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "causabas"
    df_CID10.to_sql('causabas_o', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ATESTANT = data_sim_auxiliary.get_ATESTANT_treated()
    df_ATESTANT.to_sql('atestante', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FONTEINV = data_sim_auxiliary.get_FONTEINV_treated()
    df_FONTEINV.to_sql('fonteinv', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCAGR1 = data_sim_auxiliary.get_ESCAGR1_treated()
    df_ESCAGR1.to_sql('escmaeagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "escmaearg1"
    df_ESCAGR1.to_sql('escfalagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPOBITOCOR = data_sim_auxiliary.get_TPOBITOCOR_treated()
    df_TPOBITOCOR.to_sql('tpobitocor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################
#    copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas    #
###########################################################################################################################
###########################################################################################################################
#    MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE   #
###########################################################################################################################
# Função que utiliza "copy_expert" para a inserção de dados principais e "pandas.to_sql" para a inserção
# dos respectivos metadados no banco de dados "child_db"
def insert_into_main_table_and_arquivos(file_name, directory, date_ftp, device, child_db, connection_data):
    start = time.time()
    counting_rows = pd.read_sql(f'''SELECT COUNT('NOME') FROM {child_db}.arquivos''', con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {connection_data[0]}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do sim
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:8]
    main_table = base.lower() + 'br'

    print(f'\nIniciando a lida com o arquivo {base}{state}{year}...')
    # Cria uma instância da classe "DataSimMain" do módulo "prepare_SIM" do package "data_wrangling"
    data_sim_main = DataSimMain(state, year)
    # Chama método da classe "DataSimMain" do módulo "prepare_SIM" referentes ao banco de dados sim
    df = data_sim_main.get_DOXXaaaa_treated()

    # Inserção das colunas UF_DO e ANO_DO no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int(year)]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do sim armazenado no objeto...
    # pandas DataFrame "df"
    df.to_csv(base + state + year + '.csv', sep=',', header=False, index=False, escapechar=' ')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do sim
    f = open(base + state + year + '.csv', 'r')
    # Conecta ao banco de dados mãe "connection_data[0]" do SGBD PostgreSQL usando o módulo python "psycopg2"
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])
    # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
    cursor = conn.cursor()
    try:
        # Faz a inserção dos dados armazenados em "f" na tabela "main_table" do banco de dados "child_db"...
        # usando o método "copy_expert" do "psycopg2"
        cursor.copy_expert(f'''COPY {child_db}.{main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
    except:
        print(f'Tentando a inserção do arquivo {base}{state}{year} por método alternativo (pandas)...')
        df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    else:
        conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()
    # Encerra o file handler
    f.close()
    # Remoção do arquivo "csv"
    os.remove(base + state + year  + '.csv')
    print(f'Terminou de inserir os dados do arquivo {base}{state}{year} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o...
    # arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo {base}{state}{year} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

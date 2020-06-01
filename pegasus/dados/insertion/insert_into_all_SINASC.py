###########################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC #
###########################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from transform.prepare_SINASC import DataSinascMain, DataSinascAuxiliary

###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SINASC_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataSinascAuxiliary" do módulo "prepare_SINASC" do package "data_wrangling"
    data_sinasc_auxiliary = DataSinascAuxiliary(path)

    # Chama métodos da classe "DataSinascAuxiliary" do módulo "prepare_SINASC" do banco de dados SINASC
    df_CNESDN = data_sinasc_auxiliary.get_CNESDN_treated()
    df_CNESDN.to_sql('codestab', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = data_sinasc_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_sinasc_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_sinasc_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('codmunnasc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LOCOCOR = data_sinasc_auxiliary.get_LOCOCOR_treated()
    df_LOCOCOR.to_sql('locnasc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SITCONJU = data_sinasc_auxiliary.get_SITCONJU_treated()
    df_SITCONJU.to_sql('estcivmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INSTRUC = data_sinasc_auxiliary.get_INSTRUC_treated()
    df_INSTRUC.to_sql('escmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABOCUP_2TCC = data_sinasc_auxiliary.get_TABOCUP_2TCC_treated()
    df_TABOCUP_2TCC.to_sql('codocupmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEMANAS = data_sinasc_auxiliary.get_SEMANAS_treated()
    df_SEMANAS.to_sql('gestacao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnasc"
    df_CADMUN.to_sql('codmunres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_GRAVIDEZ = data_sinasc_auxiliary.get_GRAVIDEZ_treated()
    df_GRAVIDEZ.to_sql('gravidez', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_PARTO = data_sinasc_auxiliary.get_PARTO_treated()
    df_PARTO.to_sql('parto', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CONSULT = data_sinasc_auxiliary.get_CONSULT_treated()
    df_CONSULT.to_sql('consultas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA = data_sinasc_auxiliary.get_RACA_treated()
    df_RACA.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = data_sinasc_auxiliary.get_CID10_treated()
    df_CID10.to_sql('codanomal', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NAT1212 = data_sinasc_auxiliary.get_NAT1212_treated()
    df_NAT1212.to_sql('naturalmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnasc"
    df_CADMUN.to_sql('codmunnatu', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESC2010 = data_sinasc_auxiliary.get_ESC2010_treated()
    df_ESC2010.to_sql('escmae2010', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "racacor"
    df_RACA.to_sql('racacormae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPMETODO = data_sinasc_auxiliary.get_TPMETODO_treated()
    df_TPMETODO.to_sql('tpmetestim', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPAPRESENT = data_sinasc_auxiliary.get_TPAPRESENT_treated()
    df_TPAPRESENT.to_sql('tpapresent', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_STTRABPART = data_sinasc_auxiliary.get_STTRABPART_treated()
    df_STTRABPART.to_sql('sttrabpart', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_STPARTO = data_sinasc_auxiliary.get_STPARTO_treated()
    df_STPARTO.to_sql('stcesparto', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPASSIST = data_sinasc_auxiliary.get_TPASSIST_treated()
    df_TPASSIST.to_sql('tpnascassi', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPFUNCRESP = data_sinasc_auxiliary.get_TPFUNCRESP_treated()
    df_TPFUNCRESP.to_sql('tpfuncresp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCAGR1 = data_sinasc_auxiliary.get_ESCAGR1_treated()
    df_ESCAGR1.to_sql('escmaeagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABPAIS = data_sinasc_auxiliary.get_TABPAIS_treated()
    df_TABPAIS.to_sql('codpaisres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ROBSON = data_sinasc_auxiliary.get_ROBSON_treated()
    df_ROBSON.to_sql('tprobson', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do SINASC
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:8]
    main_table = base.lower() + 'br'

    print(f'\nIniciando a lida com o arquivo DN{state}{year}...')
    # Cria uma instância da classe "DataSinascMain" do módulo "prepare_SINASC" do package "data_wrangling"
    data_sinasc_main = DataSinascMain(state, year)
    # Chama método da classe "DataSinascMain" do módulo "prepare_SINASC" referentes ao banco de dados sinasc
    df = data_sinasc_main.get_DNXXaaaa_treated()

    # Inserção das colunas UF_DN e ANO_DN no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int(year)]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do sinasc armazenado no objeto...
    # pandas DataFrame "df"
    df.to_csv(base + state + year + '.csv', sep=',', header=False, index=False, escapechar=' ')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do sinasc
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
    os.remove(base + state + year + '.csv')
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

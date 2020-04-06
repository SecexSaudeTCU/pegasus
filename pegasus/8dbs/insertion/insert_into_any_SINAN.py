############################################################################################################################################################################
#     SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY      #
############################################################################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SINAN_DENG_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    from .data_wrangling import prepare_SINAN_DENG

    # Chama funções definidas no módulo "prepare_SINAN_DENG" do package "data_wrangling"
    df_Classdeng = prepare_SINAN_DENG.get_Classdeng_treated()
    df_Classdeng.to_sql('classifin', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = prepare_SINAN_DENG.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = prepare_SINAN_DENG.get_CADMUN_treated()
    df_CADMUN.to_sql('municip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################################################################
#         copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas         #
###########################################################################################################################################################################
###########################################################################################################################################################################
#  MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE #
###########################################################################################################################################################################

# Função que utiliza "copy_expert " para a inserção de dados principais e "pandas.to_sql" para a inserção
# dos respectivos metadados no banco de dados "child_db"
def insert_into_main_table_and_arquivos(file_name, directory, date_ftp, device, child_db, connection_data):
    start = time.time()
    counting_rows = pd.read_sql(f'''SELECT COUNT('NOME') FROM {child_db}.arquivos''', con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {connection_data[0]}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do SINAN_XXXX
    base = file_name[0:4]
    state = file_name[4:6]
    year = file_name[6:8]
    main_table = base.lower() + 'br'
    counting_rows = pd.read_sql(f'''SELECT COUNT(*) from {child_db}.{main_table}''', con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo {base}{state}{year}...')

    # Criação de objeto string do nome de uma função de tratamento de dados da tabela principal "main_table"...
    # do "child_db" contida no respectivo módulo do package "data_wrangling"
    func_string = 'get_' + base + 'XXaa_treated'

    # Importação da função de tratamento de dados de uma tabela principal do "child_db" usando a função python "__import__"
    module = __import__('insertion.data_wrangling.prepare_SINAN_' + base, fromlist=[func_string], level=0)
    func_treat_main_table = getattr(module, func_string)

    # Chama a função "func_treat_main_table" do módulo "prepare_SINAN_XXXX" do package "data_wrangling"
    df = func_treat_main_table(state, year)
    # Inserção das colunas UF_XXXX e ANO_XXXX no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do SIH_XX armazenado no objeto
    # pandas DataFrame "df"
    df.to_csv(base + state + year + '.csv', sep=',', header=False, index=False)
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do SIH_XX
    f = open(base + state + year + '.csv', 'r')
    # Conecta ao banco de dados mãe "DB_NAME" do SGBD PostgreSQL usando o módulo python "psycopg2"
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
    os.remove(base + state + year + '.csv')
    print(f'Terminou de inserir os dados do arquivo {base}{state}{year} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo {base}{state}{year} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

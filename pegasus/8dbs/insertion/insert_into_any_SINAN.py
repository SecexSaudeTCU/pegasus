############################################################################################################################################################################
#     SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY SINAN_ANY      #
############################################################################################################################################################################

import time
from datetime import datetime

import numpy as np
import pandas as pd

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_SINAN_DENG_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
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
# pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################################################################
###########################################################################################################################################################################
#  MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE #
###########################################################################################################################################################################

# Função que utiliza para a inserção de dados principais da "child_db" o pandas.to_sql + SQLAlchemy
def insert_main_table_e_file_info_pandas(file_name, directory, date_ftp, device, child_db, parent_db):
    start = time.time()
    counting_rows = pd.read_sql('''SELECT COUNT('NOME') FROM %s.arquivos''' % (child_db), con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {parent_db}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do SINAN_XXXX
    base = file_name[0:4]
    state = file_name[4:6]
    year = file_name[6:8]
    main_table = base.lower() + 'br'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo {base}{state}{year}...')

    # Criação de objeto string do nome de uma função de tratamento de dados da tabela principal "main_table"...
    # do "child_db" contida no respectivo módulo do package "data_wrangling"
    func_string = 'get_' + base + 'XXaa_treated'

    # Importação da função de tratamento de dados de uma tabela principal do "child_db" usando a função python "__import__"
    module = __import__('insertion.data_wrangling.prepare_SINAN_' + base, fromlist=[func_string], level=0)
    func_treatment = getattr(module, func_string)

    # Chama a função "func_treatment" do módulo "prepare_SINAN_XXXX" do package "data_wrangling"
    df = func_treatment(state, year)
    # Inserção das colunas UF_XXXX e ANO_XXXX no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])

    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
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
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo pandas!')

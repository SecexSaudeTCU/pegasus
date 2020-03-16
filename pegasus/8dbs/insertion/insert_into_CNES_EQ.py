############################################################################################################################################################################
#  CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ #
############################################################################################################################################################################

import time

import numpy as np
import pandas as pd

from .data_wrangling.prepare_CNES_EQ import *

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_CNES_EQ_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_CNES_EQ" do package "data_wrangling"
    df_CADGERBR = get_CADGERBR_treated(path)
    df_CADGERBR.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = get_CADMUN_treated()
    df_CADMUN.to_sql('codufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_EQUIPAM = get_TP_EQUIPAM_treated()
    df_TP_EQUIPAM.to_sql('tipequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Equip_Tp = get_Equip_Tp_treated()
    df_Equip_Tp.to_sql('codequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do CNES_EQ
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = 'eqbr'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo EQ{state}{year}{month}...')
    # Chama a função "get_EQXXaamm_treated" do módulo "prepare_CNES_EQ" do package "data_wrangling"
    df = get_EQXXaamm_treated(state, year, month)
    # Inserção das colunas UF_EQ, ANO_EQ e MES_EQ no objeto pandas DataFrame "df"
    df.insert(1, 'UF_EQ', [state]*df.shape[0])
    df.insert(2, 'ANO_EQ', [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_EQ', [month]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os dados do arquivo EQ{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo EQ{state}{year}{month} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo pandas!')

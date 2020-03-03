############################################################################################################################################################################
#  CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
############################################################################################################################################################################

import time

import numpy as np
import pandas as pd

from .data_wrangling.prepare_CNES_EP import *

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_CNES_EP_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_CNES_HB" do package "data_wrangling"
    df_CADGERBR = get_CADGERBR_treated(path)
    df_CADGERBR.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = get_CADMUN_treated()
    df_CADMUN.to_sql('codufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_EQP_XX = get_EQP_XX_treated(path)
    df_EQP_XX.to_sql('idequipe', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_EQUIPE = get_EQUIPE_treated()
    df_EQUIPE.to_sql('tipoeqp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_AREA_XX = get_AREA_XX_treated(path)
    df_AREA_XX.to_sql('idarea', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEGM_XX = get_SEGM_XX_treated(path)
    df_SEGM_XX.to_sql('idsegm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_tiposegm = get_tiposegm_treated()
    df_tiposegm.to_sql('tiposegm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_motdesat = get_motdesat_treated()
    df_motdesat.to_sql('motdesat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_DESAT = get_TP_DESAT_treated()
    df_TP_DESAT.to_sql('tpdesat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)



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

    # Tratamento de dados principais do CNES_EP
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = 'epbr'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo EP{state}{year}{month}...')
    # Chama a função "get_EPXXaamm_treated" do módulo "prepare_CNES_EP" do package "data_wrangling"
    df = get_EPXXaamm_treated(state, year, month)
    # Inserção das colunas UF_EP, ANO_EP e MES_EP no objeto pandas DataFrame "df"
    df.insert(1, 'UF_EP', [state]*df.shape[0])
    df.insert(2, 'ANO_EP', [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_EP', [month]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os dados do arquivo EP{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir informações do arquivo principal de dados na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo SQLAlchemy-pandas!')

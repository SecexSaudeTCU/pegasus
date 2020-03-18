############################################################################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC #
############################################################################################################################################################################

import time
from datetime import datetime

import numpy as np
import pandas as pd

from .data_wrangling import prepare_SINASC

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_SINASC_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_SINASC" do package "data_wrangling"

    df_CNESDN = prepare_SINASC.get_CNESDN_treated(path)
    df_CNESDN.to_sql('codestab', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = prepare_SINASC.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = prepare_SINASC.get_CADMUN_treated()
    df_CADMUN.to_sql('codmunnasc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LOCOCOR = prepare_SINASC.get_LOCOCOR_treated()
    df_LOCOCOR.to_sql('locnasc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SITCONJU = prepare_SINASC.get_SITCONJU_treated()
    df_SITCONJU.to_sql('estcivmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INSTRUC = prepare_SINASC.get_INSTRUC_treated()
    df_INSTRUC.to_sql('escmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABOCUP_2TCC = prepare_SINASC.get_TABOCUP_2TCC_treated(path)
    df_TABOCUP_2TCC.to_sql('codocupmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEMANAS = prepare_SINASC.get_SEMANAS_treated()
    df_SEMANAS.to_sql('gestacao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnasc"
    df_CADMUN.to_sql('codmunres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_GRAVIDEZ = prepare_SINASC.get_GRAVIDEZ_treated()
    df_GRAVIDEZ.to_sql('gravidez', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_PARTO = prepare_SINASC.get_PARTO_treated()
    df_PARTO.to_sql('parto', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CONSULT = prepare_SINASC.get_CONSULT_treated()
    df_CONSULT.to_sql('consultas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA = prepare_SINASC.get_RACA_treated()
    df_RACA.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = prepare_SINASC.get_CID10_treated()
    df_CID10.to_sql('codanomal', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NAT1212 = prepare_SINASC.get_NAT1212_treated()
    df_NAT1212.to_sql('naturalmae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "codmunnasc"
    df_CADMUN.to_sql('codmunnatu', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESC2010 = prepare_SINASC.get_ESC2010_treated()
    df_ESC2010.to_sql('escmae2010', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "racacor"
    df_RACA.to_sql('racacormae', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPMETODO = prepare_SINASC.get_TPMETODO_treated()
    df_TPMETODO.to_sql('tpmetestim', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPAPRESENT = prepare_SINASC.get_TPAPRESENT_treated(path)
    df_TPAPRESENT.to_sql('tpapresent', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_STTRABPART = prepare_SINASC.get_STTRABPART_treated(path)
    df_STTRABPART.to_sql('sttrabpart', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_STPARTO = prepare_SINASC.get_STPARTO_treated()
    df_STPARTO.to_sql('stcesparto', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPASSIST = prepare_SINASC.get_TPASSIST_treated()
    df_TPASSIST.to_sql('tpnascassi', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPFUNCRESP = prepare_SINASC.get_TPFUNCRESP_treated(path)
    df_TPFUNCRESP.to_sql('tpfuncresp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESCAGR1 = prepare_SINASC.get_ESCAGR1_treated()
    df_ESCAGR1.to_sql('escmaeagr1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABPAIS = prepare_SINASC.get_TABPAIS_treated()
    df_TABPAIS.to_sql('codpaisres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ROBSON = prepare_SINASC.get_ROBSON_treated()
    df_ROBSON.to_sql('tprobson', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do SINASC
    state = file_name[2:4]
    year = file_name[4:8]
    main_table = 'dnbr'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo DN{state}{year}...')
    # Chama a função "get_DNXXaaaa_treated" do módulo "prepare_SINASC" do package "data_wrangling"
    df = prepare_SINASC.get_DNXXaaaa_treated(state, year)
    # Inserção das colunas UF_DN e ANO_DN no objeto pandas DataFrame "df"
    df.insert(1, 'UF_DN', [state]*df.shape[0])
    df.insert(2, 'ANO_DN', [int(year)]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os dados do arquivo DN{state}{year} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo DN{state}{year} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo SQLAlchemy-pandas!')

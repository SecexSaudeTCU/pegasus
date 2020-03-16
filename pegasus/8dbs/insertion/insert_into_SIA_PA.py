############################################################################################################################################################################
#  SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
############################################################################################################################################################################

import time

import numpy as np
import pandas as pd

from .data_wrangling.prepare_SIA_PA import *

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_SIA_PA_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_SIA_PA" do package "data_wrangling"

    df_CADGERBR = get_CADGERBR_treated(path)
    df_CADGERBR.to_sql('pacoduni', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = get_CADMUN_treated()
    df_CADMUN.to_sql('pagestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_GESTAO = get_TP_GESTAO_treated()
    df_TP_GESTAO.to_sql('pacondic', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pagestao"
    df_CADMUN.to_sql('paufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_REGRA_C = get_REGRA_C_treated()
    df_REGRA_C.to_sql('paregct', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_ESTAB = get_TP_ESTAB_treated()
    df_TP_ESTAB.to_sql('patpups', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESFERA = get_ESFERA_treated()
    df_ESFERA.to_sql('patippre', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SIGTAP = get_TB_SIGTAP_treated(path)
    df_TB_SIGTAP.to_sql('paproc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = get_FINANC_treated()
    df_FINANC.to_sql('patpfin', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX = get_COMPLEX_treated()
    df_COMPLEX.to_sql('panivcpl', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_DOCORIG = get_DOCORIG_treated()
    df_DOCORIG.to_sql('padocorig', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = get_CBO_treated()
    df_CBO.to_sql('pacbocod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_MOTSAIPE = get_MOTSAIPE_treated()
    df_MOTSAIPE.to_sql('pamotsai', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_S_CID = get_S_CID_treated()
    df_S_CID.to_sql('pacidpri', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacidpri"
    df_S_CID.to_sql('pacidsec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacidpri"
    df_S_CID.to_sql('pacidcas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CARAT_AT = get_CARAT_AT_treated()
    df_CARAT_AT.to_sql('pacatend', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FL_IDADE = get_FL_IDADE_treated()
    df_FL_IDADE.to_sql('paflidade', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEXO = get_SEXO_treated()
    df_SEXO.to_sql('pasexo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA_COR = get_RACA_COR_treated()
    df_RACA_COR.to_sql('paracacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pagestao"
    df_CADMUN.to_sql('pamunpcn', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INDICA = get_INDICA_treated()
    df_INDICA.to_sql('paindica', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CODOCO = get_CODOCO_treated()
    df_CODOCO.to_sql('pacodoco', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_etnia = get_etnia_treated()
    df_etnia.to_sql('paetnia', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacbocod"
    df_CBO.to_sql('pasrcc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INE_EQUIPE_BR = get_INE_EQUIPE_BR_treated()
    df_INE_EQUIPE_BR.to_sql('paine', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_natjur = get_natjur_treated()
    df_natjur.to_sql('panatjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do SIA_PA
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    if ((state + year + month == 'SP1112') or ((state == 'SP') and (int(year) >= 13))):
        month = file_name[6:9]
    main_table = 'pabr'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo PA{state}{year}{month}...')
    # Chama a função "get_PAXXaamm_treated" do módulo "prepare_SIA_PA" do package "data_wrangling"
    df = get_PAXXaamm_treated(state, year, month)
    # Inserção das colunas UF_PA, ANO_PA e MES_PA no objeto pandas DataFrame "df"
    df.insert(1, 'UF_PA', [state]*df.shape[0])
    df.insert(2, 'ANO_PA', [int('20' + year)]*df.shape[0])
    if ((state + year + month == 'SP1112') or ((state == 'SP') and (int(year) >= 13))):
        df.insert(3, 'MES_PA', [month[0:2]]*df.shape[0])
    else:
        df.insert(3, 'MES_PA', [month]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os dados do arquivo PA{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo PA{state}{year}{month} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo pandas!')

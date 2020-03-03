############################################################################################################################################################################
#  SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD #
############################################################################################################################################################################

import time

import numpy as np
import pandas as pd

from .data_wrangling.prepare_SIH_RD import *

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_SIH_RD_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_SIH_RD" do package "data_wrangling"

    df_TABUF = get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = get_CADMUN_treated()
    df_CADMUN.to_sql('ufzi', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LEITOS = get_LEITOS_treated()
    df_LEITOS.to_sql('espec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_IDENT = get_IDENT_treated()
    df_IDENT.to_sql('ident', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "ufzi"
    df_CADMUN.to_sql('municres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_MARCAUTI = get_MARCAUTI_treated()
    df_MARCAUTI.to_sql('marcauti', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SIGTAP = get_TB_SIGTAP_treated(path)
    df_TB_SIGTAP.to_sql('procsolic', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "procsolic"
    df_TB_SIGTAP.to_sql('procrea', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = get_CID10_treated()
    df_CID10.to_sql('diagprinc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SAIDAPERM = get_SAIDAPERM_treated()
    df_SAIDAPERM.to_sql('cobranca', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATUREZA = get_NATUREZA_treated()
    df_NATUREZA.to_sql('natureza', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_natjur = get_natjur_treated()
    df_natjur.to_sql('natjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_GESTAO = get_GESTAO_treated()
    df_GESTAO.to_sql('gestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "ufzi"
    df_CADMUN.to_sql('municmov', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NACION3D = get_NACION3D_treated()
    df_NACION3D.to_sql('nacional', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CARATEND = get_CARATEND_treated()
    df_CARATEND.to_sql('carint', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INSTRU = get_INSTRU_treated()
    df_INSTRU.to_sql('instru', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CONTRAC = get_CONTRAC_treated()
    df_CONTRAC.to_sql('contracep1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "contracep1"
    df_CONTRAC.to_sql('contracep2', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = get_CBO_treated()
    df_CBO.to_sql('cbor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNAE = get_CNAE_treated()
    df_CNAE.to_sql('cnaer', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_VINCPREV = get_VINCPREV_treated()
    df_VINCPREV.to_sql('vincprev', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNES = get_CNES_treated(path)
    df_CNES.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX2 = get_COMPLEX2_treated()
    df_COMPLEX2.to_sql('complex', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = get_FINANC_treated()
    df_FINANC.to_sql('financ', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FAECTP = get_FAECTP_treated()
    df_FAECTP.to_sql('faectp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_REGCT = get_REGCT_treated()
    df_REGCT.to_sql('regct', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACACOR = get_RACACOR_treated()
    df_RACACOR.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_etnia = get_etnia_treated()
    df_etnia.to_sql('etnia', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do SIH_RD
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = 'rdbr'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo RD{state}{year}{month}...')
    # Chama a função "get_RDXXaamm_treated" do módulo "prepare_SIH_RD" do package "data_wrangling"
    df = get_RDXXaamm_treated(state, year, month)
    # Inserção das colunas UF_RD, ANO_RD e MES_RD no objeto pandas DataFrame "df"
    df.insert(1, 'UF_RD', [state]*df.shape[0])
    df.insert(2, 'ANO_RD', [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_RD', [month]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os dados do arquivo RD{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

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

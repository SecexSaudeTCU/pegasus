###########################################################################################################################
# SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY SIH_ANY #
###########################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from transform.prepare_SIH import DataSihMain, DataSihAuxiliary

###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SIH_RD_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataSihAuxiliary" do módulo "prepare_SIH" do package "data_wrangling"
    data_sih_auxiliary = DataSihAuxiliary(path)

    # Chama métodos da classe "DataSihAuxiliary" do módulo "prepare_SIH" referentes ao sub-banco de dados sih_rd
    df_TABUF = data_sih_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_sih_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_sih_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('ufzi', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LEITOS = data_sih_auxiliary.get_LEITOS_treated()
    df_LEITOS.to_sql('espec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_IDENT = data_sih_auxiliary.get_IDENT_treated()
    df_IDENT.to_sql('ident', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "ufzi"
    df_CADMUN.to_sql('municres', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_MARCAUTI = data_sih_auxiliary.get_MARCAUTI_treated()
    df_MARCAUTI.to_sql('marcauti', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SIGTAP = data_sih_auxiliary.get_TB_SIGTAP_treated()
    df_TB_SIGTAP.to_sql('procsolic', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "procsolic"
    df_TB_SIGTAP.to_sql('procrea', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = data_sih_auxiliary.get_CID10_treated()
    df_CID10.to_sql('diagprinc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SAIDAPERM = data_sih_auxiliary.get_SAIDAPERM_treated()
    df_SAIDAPERM.to_sql('cobranca', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATUREZA = data_sih_auxiliary.get_NATUREZA_treated()
    df_NATUREZA.to_sql('natureza', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_natjur = data_sih_auxiliary.get_natjur_treated()
    df_natjur.to_sql('natjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_GESTAO = data_sih_auxiliary.get_GESTAO_treated()
    df_GESTAO.to_sql('gestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "ufzi"
    df_CADMUN.to_sql('municmov', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NACION3D = data_sih_auxiliary.get_NACION3D_treated()
    df_NACION3D.to_sql('nacional', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CARATEND = data_sih_auxiliary.get_CARATEND_treated()
    df_CARATEND.to_sql('carint', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INSTRU = data_sih_auxiliary.get_INSTRU_treated()
    df_INSTRU.to_sql('instru', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CONTRAC = data_sih_auxiliary.get_CONTRAC_treated()
    df_CONTRAC.to_sql('contracep1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "contracep1"
    df_CONTRAC.to_sql('contracep2', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = data_sih_auxiliary.get_CBO_treated()
    df_CBO.to_sql('cbor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNAE = data_sih_auxiliary.get_CNAE_treated()
    df_CNAE.to_sql('cnaer', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_VINCPREV = data_sih_auxiliary.get_VINCPREV_treated()
    df_VINCPREV.to_sql('vincprev', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNES = data_sih_auxiliary.get_CNES_treated()
    df_CNES.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX2 = data_sih_auxiliary.get_COMPLEX2_treated()
    df_COMPLEX2.to_sql('complex', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = data_sih_auxiliary.get_FINANC_treated()
    df_FINANC.to_sql('financ', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FAECTP = data_sih_auxiliary.get_FAECTP_treated()
    df_FAECTP.to_sql('faectp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_REGCT = data_sih_auxiliary.get_REGCT_treated()
    df_REGCT.to_sql('regct', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACACOR = data_sih_auxiliary.get_RACACOR_treated()
    df_RACACOR.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_etnia = data_sih_auxiliary.get_etnia_treated()
    df_etnia.to_sql('etnia', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_GRUPO = data_sih_auxiliary.get_TB_GRUPO_treated()
    df_TB_GRUPO.to_sql('grupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SUBGR = data_sih_auxiliary.get_TB_SUBGR_treated()
    df_TB_SUBGR.to_sql('subgrupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_FORMA = data_sih_auxiliary.get_TB_FORMA_treated()
    df_TB_FORMA.to_sql('forma', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SIH_SP_tables(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataSihAuxiliary" do módulo "prepare_SIH" do package "data_wrangling"
    data_sih_auxiliary = DataSihAuxiliary(path)

    # Chama métodos da classe "DataSihAuxiliary" do módulo "prepare_SIH" referentes ao sub-banco de dados sih_sp
    df_TB_SIGTAP = data_sih_auxiliary.get_TB_SIGTAP_treated()
    df_TB_SIGTAP.to_sql('spprocrea', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = data_sih_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_sih_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_sih_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('spgestor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNES = data_sih_auxiliary.get_CNES_treated()
    df_CNES.to_sql('spcnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "spprocrea"
    df_TB_SIGTAP.to_sql('spatoprof', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "spgestor"
    df_CADMUN.to_sql('spmhosp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "spgestor"
    df_CADMUN.to_sql('spmpac', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX2 = data_sih_auxiliary.get_COMPLEX2_treated()
    df_COMPLEX2.to_sql('spcomplex', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = data_sih_auxiliary.get_FINANC_treated()
    df_FINANC.to_sql('spfinanc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FAECTP = data_sih_auxiliary.get_FAECTP_treated()
    df_FAECTP.to_sql('spcofaec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = data_sih_auxiliary.get_CBO_treated()
    df_CBO.to_sql('sppfcbo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_VAL = data_sih_auxiliary.get_TP_VAL_treated()
    df_TP_VAL.to_sql('intpval', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_S_CLASSEN = data_sih_auxiliary.get_S_CLASSEN_treated()
    df_S_CLASSEN.to_sql('servcla', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = data_sih_auxiliary.get_CID10_treated()
    df_CID10.to_sql('spcidpri', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "spcidpri"
    df_CID10.to_sql('spcidsec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_GRUPO = data_sih_auxiliary.get_TB_GRUPO_treated()
    df_TB_GRUPO.to_sql('grupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SUBGR = data_sih_auxiliary.get_TB_SUBGR_treated()
    df_TB_SUBGR.to_sql('subgrupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_FORMA = data_sih_auxiliary.get_TB_FORMA_treated()
    df_TB_FORMA.to_sql('forma', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do sih_xx
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = base.lower() + 'br'

    print(f'\nIniciando a lida com o arquivo {base}{state}{year}{month}...')
    # Cria uma instância da classe "DataSihMain" do módulo "prepare_SIH" do package "data_wrangling"
    data_sih_main = DataSihMain(base, state, year, month)
    # Chama método da classe "DataSihMain" do módulo "prepare_SIH" referentes ao sub-banco de dados sih_xx
    df = data_sih_main.get_SIHXXaamm_treated()

    # Inserção das colunas UF_XX, ANO_XX, MES_XX e CONTAGEM no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_' + base, [month]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do sih_xx armazenado no...
    # objeto pandas DataFrame "df"
    df.to_csv(base + state + year + month + '.csv', sep=',', header=False, index=False, escapechar=' ')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do sih_xx
    f = open(base + state + year + month + '.csv', 'r')
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
        print(f'Tentando a inserção do arquivo {base}{state}{year}{month} por método alternativo (pandas)...')
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
    os.remove(base + state + year + month + '.csv')
    print(f'Terminou de inserir os dados do arquivo {base}{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o...
    # arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo {base}{state}{year}{month} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start), 1)} segundos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

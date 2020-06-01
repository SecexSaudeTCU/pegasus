###########################################################################################################################
# SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY SIA_ANY #
###########################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from transform.prepare_SIA import DataSiaMain, DataSiaAuxiliary

###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_into_most_SIA_PA_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataSiaAuxiliary" do módulo "prepare_SIA" do package "data_wrangling"
    data_sia_auxiliary = DataSiaAuxiliary(path)

    # Chama métodos da classe "DataSiaAuxiliary" do módulo "prepare_SIA" referentes ao sub-banco de dados sia_pa
    df_CADGERBR = data_sia_auxiliary.get_CADGERBR_treated()
    df_CADGERBR.to_sql('pacoduni', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = data_sia_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_sia_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_sia_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('pagestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_GESTAO = data_sia_auxiliary.get_TP_GESTAO_treated()
    df_TP_GESTAO.to_sql('pacondic', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pagestao"
    df_CADMUN.to_sql('paufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_REGRA_C = data_sia_auxiliary.get_REGRA_C_treated()
    df_REGRA_C.to_sql('paregct', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_ESTAB = data_sia_auxiliary.get_TP_ESTAB_treated()
    df_TP_ESTAB.to_sql('patpups', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ESFERA = data_sia_auxiliary.get_ESFERA_treated()
    df_ESFERA.to_sql('patippre', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SIGTAP = data_sia_auxiliary.get_TB_SIGTAP_treated()
    df_TB_SIGTAP.to_sql('paproc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = data_sia_auxiliary.get_FINANC_treated()
    df_FINANC.to_sql('patpfin', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX = data_sia_auxiliary.get_COMPLEX_treated()
    df_COMPLEX.to_sql('panivcpl', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_DOCORIG = data_sia_auxiliary.get_DOCORIG_treated()
    df_DOCORIG.to_sql('padocorig', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = data_sia_auxiliary.get_CBO_treated()
    df_CBO.to_sql('pacbocod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_MOTSAIPE = data_sia_auxiliary.get_MOTSAIPE_treated()
    df_MOTSAIPE.to_sql('pamotsai', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_S_CID = data_sia_auxiliary.get_S_CID_treated()
    df_S_CID.to_sql('pacidpri', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacidpri"
    df_S_CID.to_sql('pacidsec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacidpri"
    df_S_CID.to_sql('pacidcas', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CARAT_AT = data_sia_auxiliary.get_CARAT_AT_treated()
    df_CARAT_AT.to_sql('pacatend', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FL_IDADE = data_sia_auxiliary.get_FL_IDADE_treated()
    df_FL_IDADE.to_sql('paflidade', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEXO = data_sia_auxiliary.get_SEXO_treated()
    df_SEXO.to_sql('pasexo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACA_COR = data_sia_auxiliary.get_RACA_COR_treated()
    df_RACA_COR.to_sql('paracacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pagestao"
    df_CADMUN.to_sql('pamunpcn', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INDICA = data_sia_auxiliary.get_INDICA_treated()
    df_INDICA.to_sql('paindica', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CODOCO = data_sia_auxiliary.get_CODOCO_treated()
    df_CODOCO.to_sql('pacodoco', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_etnia = data_sia_auxiliary.get_etnia_treated()
    df_etnia.to_sql('paetnia', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Mesmo objeto pandas DataFrame da tabela "pacbocod"
    df_CBO.to_sql('pasrcc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INE_EQUIPE_BR = data_sia_auxiliary.get_INE_EQUIPE_BR_treated()
    df_INE_EQUIPE_BR.to_sql('paine', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_natjur = data_sia_auxiliary.get_natjur_treated()
    df_natjur.to_sql('panatjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_GRUPO = data_sia_auxiliary.get_TB_GRUPO_treated()
    df_TB_GRUPO.to_sql('grupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SUBGR = data_sia_auxiliary.get_TB_SUBGR_treated()
    df_TB_SUBGR.to_sql('subgrupo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_FORMA = data_sia_auxiliary.get_TB_FORMA_treated()
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

    # Tratamento de dados principais do sia_xx
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    # No mês de dez/2011 e em todos os meses a partir de jan/2013 os arquivos "dbc" PAXXaamm se dividem em...
    # dois por mês tendo no final "appended" a string "a" ou "b"
    if ((state + year + month == 'SP1112') or ((state == 'SP') and (int(year) >= 13))):
        month = file_name[6:9]
    main_table = base.lower() + 'br'

    print(f'\nIniciando a lida com o arquivo {base}{state}{year}{month}...')
    # Cria uma instância da classe "DataSiaMain" do módulo "prepare_SIA" do package "data_wrangling"
    data_sia_main = DataSiaMain(base, state, year, month)
    # Chama método da classe "DataSiaMain" do módulo "prepare_SIA" referentes ao sub-banco de dados sia_xx
    df = data_sia_main.get_SIAXXaamm_treated()

    # Inserção das colunas UF_XX, ANO_XX e MES_XX no objeto pandas DataFrame "df"
    df.insert(0, 'UF_' + base, [state]*df.shape[0])
    df.insert(1, 'ANO_' + base, [int('20' + year)]*df.shape[0])
    # No mês de dez/2011 e em todos os meses a partir de jan/2013 os arquivos "dbc" PAXXaamm se dividem em...
    # dois por mês tendo no final "appended" a string "a" ou "b"
    if ((state + year + month == 'SP1112') or ((state == 'SP') and (int(year) >= 13))):
        df.insert(2, 'MES_PA', [month[0:2]]*df.shape[0])
    else:
        df.insert(2, 'MES_PA', [month]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do sia_xx armazenado no objeto...
    # pandas DataFrame "df"
    df.to_csv(base + state + year + month + '.csv', sep=',', header=False, index=False, escapechar=' ')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do sia_xx
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
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

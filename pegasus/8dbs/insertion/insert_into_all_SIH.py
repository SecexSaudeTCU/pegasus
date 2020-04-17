############################################################################################################################################################################
# SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL SIH_ALL  #
############################################################################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from .data_wrangling import prepare_SIH_RD
from .data_wrangling import prepare_SIH_SP

############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_SIH_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_SIH_RD" do package "data_wrangling"
    df_IDENT = prepare_SIH_RD.get_IDENT_treated()
    df_IDENT.to_sql('ident', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = prepare_SIH_RD.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = prepare_SIH_RD.get_CADMUN_treated()
    df_CADMUN.to_sql('ufzi', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_LEITOS = prepare_SIH_RD.get_LEITOS_treated()
    df_LEITOS.to_sql('espec', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_MARCAUTI = prepare_SIH_RD.get_MARCAUTI_treated()
    df_MARCAUTI.to_sql('marcauti', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TB_SIGTAP = prepare_SIH_RD.get_TB_SIGTAP_treated(path)
    df_TB_SIGTAP.to_sql('procsolic', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CID10 = prepare_SIH_RD.get_CID10_treated()
    df_CID10.to_sql('diagprinc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SAIDAPERM = prepare_SIH_RD.get_SAIDAPERM_treated()
    df_SAIDAPERM.to_sql('cobranca', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATUREZA = prepare_SIH_RD.get_NATUREZA_treated()
    df_NATUREZA.to_sql('natureza', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_natjur = prepare_SIH_RD.get_natjur_treated()
    df_natjur.to_sql('natjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_GESTAO = prepare_SIH_RD.get_GESTAO_treated()
    df_GESTAO.to_sql('gestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NACION3D = prepare_SIH_RD.get_NACION3D_treated()
    df_NACION3D.to_sql('nacional', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CARATEND = prepare_SIH_RD.get_CARATEND_treated()
    df_CARATEND.to_sql('carint', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_INSTRU = prepare_SIH_RD.get_INSTRU_treated()
    df_INSTRU.to_sql('instru', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CONTRAC = prepare_SIH_RD.get_CONTRAC_treated()
    df_CONTRAC.to_sql('contracep1', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO = prepare_SIH_RD.get_CBO_treated()
    df_CBO.to_sql('cbor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNAE = prepare_SIH_RD.get_CNAE_treated()
    df_CNAE.to_sql('cnaer', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_VINCPREV = prepare_SIH_RD.get_VINCPREV_treated()
    df_VINCPREV.to_sql('vincprev', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CNES = prepare_SIH_RD.get_CNES_treated(path)
    df_CNES.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_COMPLEX2 = prepare_SIH_RD.get_COMPLEX2_treated()
    df_COMPLEX2.to_sql('complex', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FINANC = prepare_SIH_RD.get_FINANC_treated()
    df_FINANC.to_sql('financ', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_FAECTP = prepare_SIH_RD.get_FAECTP_treated()
    df_FAECTP.to_sql('faectp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_REGCT = prepare_SIH_RD.get_REGCT_treated()
    df_REGCT.to_sql('regct', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RACACOR = prepare_SIH_RD.get_RACACOR_treated()
    df_RACACOR.to_sql('racacor', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_etnia = prepare_SIH_RD.get_etnia_treated()
    df_etnia.to_sql('etnia', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_SIH_SP" do package "data_wrangling"
    df_TP_VAL = prepare_SIH_SP.get_TP_VAL_treated()
    df_TP_VAL.to_sql('intpval', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_S_CLASSEN = prepare_SIH_SP.get_S_CLASSEN_treated(path)
    df_S_CLASSEN.to_sql('servcla', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################################################################
#         copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas copy_expert+pandas         #
###########################################################################################################################################################################
###########################################################################################################################################################################
#  MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE #
###########################################################################################################################################################################

# Função que utiliza "copy_expert" para a inserção de dados principais e "pandas.to_sql" para a inserção
# dos respectivos metadados no banco de dados "child_db"
def insert_into_main_table_and_arquivos(file_name, directory, date_ftp, device, child_db, connection_data):
    start = time.time()
    counting_rows = pd.read_sql(f'''SELECT COUNT('NOME') FROM {child_db}.arquivos''', con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {connection_data[0]}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do SIH
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = base.lower() + 'br'
    counting_rows = pd.read_sql(f'''SELECT COUNT(*) from {child_db}.{main_table}''', con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo {base}{state}{year}{month}...')

    # Criação de objeto string do nome de uma função de tratamento de dados de uma tabela principal...
    # do "child_db" contida no respectivo módulo do package "data_wrangling"
    func_string = 'get_' + base + 'XXaamm_treated'

    # Importação da função de tratamento de dados de uma tabela principal do "child_db" usando a função python "__import__"
    module = __import__('insertion.data_wrangling.prepare_SIH_' + base, fromlist=[func_string], level=0)
    func_treat_main_table = getattr(module, func_string)

    # Chama a função "func_treat_main_table" do módulo "prepare_SIH_XX" do package "data_wrangling"
    df = func_treat_main_table(state, year, month)
    # Inserção das colunas UF_XX, ANO_XX, MES_XX e CONTAGEM no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_' + base, [month]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do SIH_XX armazenado no objeto
    # pandas DataFrame "df"
    df.to_csv(base + state + year + month + '.csv', sep=',', header=False, index=False)
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do SIH_XX
    f = open(base + state + year + month + '.csv', 'r')
    # Conecta ao banco de dados mãe "connection_data[0]" do SGBD PostgreSQL usando o módulo python "psycopg2"
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
    os.remove(base + state + year + month + '.csv')
    print(f'Terminou de inserir os dados do arquivo {base}{state}{year}{month} na tabela {main_table} do banco de dados {child_db}.')

    # Cria um objeto pandas DataFrame com apenas uma linha de dados, a qual contém informações sobre o arquivo de dados principal carregado
    file_data = pd.DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
    # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
    file_data.to_sql('arquivos', con=device, schema=child_db, if_exists='append', index=False)
    print(f'Terminou de inserir os metadados do arquivo {base}{state}{year}{month} na tabela arquivos do banco de dados {child_db}.')
    end = time.time()
    print(f'Demorou {round((end - start), 1)} segundos para essas duas inserções no {connection_data[0]}/PostgreSQL!')

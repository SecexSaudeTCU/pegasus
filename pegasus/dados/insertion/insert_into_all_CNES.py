###########################################################################################################################
#  CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL CNES_ALL   #
###########################################################################################################################

import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2

from transform.prepare_CNES import DataCnesMain, DataCnesAuxiliary

###########################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################
###########################################################################################################################
#     AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES     #
###########################################################################################################################
# Função que utiliza "pandas.to_sql" para a inserção de dados não principais no banco de dados "child_db"
def insert_into_most_CNES_tables(path, device, child_db):

    label1 = 'append'
    label2 = 'ID'

    # Cria uma instância da classe "DataCnesAuxiliary" do módulo "prepare_CNES" do package "data_wrangling"
    data_cnes_auxiliary = DataCnesAuxiliary(path)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" comuns a mais de um sub-banco de dados do CNES
    df_CADGERBR = data_cnes_auxiliary.get_CADGERBR_treated()
    df_CADGERBR.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = data_cnes_auxiliary.get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RSAUDE = data_cnes_auxiliary.get_RSAUDE_treated()
    df_RSAUDE.to_sql('rsaude', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = data_cnes_auxiliary.get_CADMUN_treated()
    df_CADMUN.to_sql('codufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_PFPJ = data_cnes_auxiliary.get_TP_PFPJ_treated()
    df_TP_PFPJ.to_sql('pfpj', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NIVELDEP = data_cnes_auxiliary.get_NIVELDEP_treated()
    df_NIVELDEP.to_sql('nivdep', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPGESTAO = data_cnes_auxiliary.get_TPGESTAO_treated()
    df_TPGESTAO.to_sql('tpgestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_EsferAdm = data_cnes_auxiliary.get_EsferAdm_treated()
    df_EsferAdm.to_sql('esferaa', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RETENCAO = data_cnes_auxiliary.get_RETENCAO_treated()
    df_RETENCAO.to_sql('retencao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Ativ_Ens = data_cnes_auxiliary.get_Ativ_Ens_treated()
    df_Ativ_Ens.to_sql('atividad', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATUREZA = data_cnes_auxiliary.get_NATUREZA_treated()
    df_NATUREZA.to_sql('natureza', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Flux_Cli = data_cnes_auxiliary.get_Flux_Cli_treated()
    df_Flux_Cli.to_sql('clientel', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_ESTAB = data_cnes_auxiliary.get_TP_ESTAB_treated()
    df_TP_ESTAB.to_sql('tpunid', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TurnosAt = data_cnes_auxiliary.get_TurnosAt_treated()
    df_TurnosAt.to_sql('turnoat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NIV_HIER = data_cnes_auxiliary.get_NIV_HIER_treated()
    df_NIV_HIER.to_sql('nivhier', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TIPOPRES = data_cnes_auxiliary.get_TIPOPRES_treated()
    df_TIPOPRES.to_sql('tpprest', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATJUR = data_cnes_auxiliary.get_NATJUR_treated()
    df_NATJUR.to_sql('natjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_st
    df_RETENMAN = data_cnes_auxiliary.get_RETENMAN_treated()
    df_RETENMAN.to_sql('codir', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ORGEXPED = data_cnes_auxiliary.get_ORGEXPED_treated()
    df_ORGEXPED.to_sql('orgexped', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CLASAVAL = data_cnes_auxiliary.get_CLASAVAL_treated()
    df_CLASAVAL.to_sql('clasaval', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_pf
    df_CBO = data_cnes_auxiliary.get_CBO_treated()
    df_CBO.to_sql('cbo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO.to_sql('cbounico', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CR_CONSEL = data_cnes_auxiliary.get_CR_CONSEL_treated()
    df_CR_CONSEL.to_sql('conselho', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_VINCULO = data_cnes_auxiliary.get_VINCULO_treated()
    df_VINCULO.to_sql('vinculac', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_lt
    df_tip1leit = data_cnes_auxiliary.get_tip1leit_treated()
    df_tip1leit.to_sql('tpleito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Esp_leit = data_cnes_auxiliary.get_Esp_leit_treated()
    df_Esp_leit.to_sql('codleito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_eq
    df_TP_EQUIPAM = data_cnes_auxiliary.get_TP_EQUIPAM_treated()
    df_TP_EQUIPAM.to_sql('tipequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Equip_Tp = data_cnes_auxiliary.get_Equip_Tp_treated()
    df_Equip_Tp.to_sql('codequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_sr
    df_SERVICO = data_cnes_auxiliary.get_SERVICO_treated()
    df_SERVICO.to_sql('servesp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CLASSSR = data_cnes_auxiliary.get_CLASSSR_treated()
    df_CLASSSR.to_sql('classsr', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SERVICO.to_sql('srvunico', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Srv_Caract = data_cnes_auxiliary.get_Srv_Caract_treated()
    df_Srv_Caract.to_sql('caracter', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama métodos da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referentes ao sub-banco de dados cnes_ep
    df_EQP_XX = data_cnes_auxiliary.get_EQP_XX_treated()
    df_EQP_XX.to_sql('idequipe', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_EQUIPE = data_cnes_auxiliary.get_EQUIPE_treated()
    df_EQUIPE.to_sql('tipoeqp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_AREA_XX = data_cnes_auxiliary.get_AREA_XX_treated()
    df_AREA_XX.to_sql('idarea', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SEGM_XX = data_cnes_auxiliary.get_SEGM_XX_treated()
    df_SEGM_XX.to_sql('idsegm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_tiposegm = data_cnes_auxiliary.get_tiposegm_treated()
    df_tiposegm.to_sql('tiposegm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_motdesat = data_cnes_auxiliary.get_motdesat_treated()
    df_motdesat.to_sql('motdesat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_DESAT = data_cnes_auxiliary.get_TP_DESAT_treated()
    df_TP_DESAT.to_sql('tpdesat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama método da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referente ao sub-banco de dados cnes_hb
    df_HABILITA = data_cnes_auxiliary.get_HABILITA_treated()
    df_HABILITA.to_sql('sgruphab_hb', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama método da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referente ao sub-banco de dados cnes_rc
    df_REGRAS = data_cnes_auxiliary.get_REGRAS_treated()
    df_REGRAS.to_sql('sgruphab_rc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama método da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referente ao sub-banco de dados cnes_gm
    df_GESTAO = data_cnes_auxiliary.get_GESTAO_treated()
    df_GESTAO.to_sql('sgruphab_gm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama método da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referente ao sub-banco de dados cnes_ee
    df_ESTABENS = data_cnes_auxiliary.get_ESTABENS_treated()
    df_ESTABENS.to_sql('sgruphab_ee', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama método da classe "DataCnesAuxiliary" do módulo "prepare_CNES" referente ao sub-banco de dados cnes_ef
    df_ESTABFIL = data_cnes_auxiliary.get_ESTABFIL_treated()
    df_ESTABFIL.to_sql('sgruphab_ef', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


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

    # Tratamento de dados principais do cnes
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = base.lower() + 'br'

    print(f'\nIniciando a lida com o arquivo {base}{state}{year}{month}.')
    # Cria uma instância da classe "DataCnesMain" do módulo "prepare_CNES" do package "data_wrangling"
    data_cnes_main = DataCnesMain(base, state, year, month)
    # Chama método da classe "DataCnesMain" do módulo "prepare_CNES" referentes ao banco de dados cnes
    df = data_cnes_main.get_CNESXXaamm_treated()

    # Inserção das colunas UF_XX, ANO_XX e MES_XX no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_' + base, [month]*df.shape[0])

    # Criação de arquivo "csv" contendo os dados do arquivo principal de dados do cnes armazenado no objeto...
    # pandas DataFrame "df"
    df.to_csv(base + state + year + month + '.csv', sep=',', header=False, index=False, escapechar=' ')
    # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do cnes
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

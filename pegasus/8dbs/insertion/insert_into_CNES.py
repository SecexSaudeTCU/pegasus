############################################################################################################################################################################
#   CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES   #
############################################################################################################################################################################

import time
from datetime import datetime

import numpy as np
import pandas as pd

from .data_wrangling.prepare_CNES_ST import (get_CADGERBR_treated, get_TABUF_treated, get_CADMUN_treated,
                                            get_TP_PFPJ_treated,  get_NIVELDEP_treated, get_RETENMAN_treated,
                                            get_TPGESTAO_treated, get_EsferAdm_treated, get_Ativ_Ens_treated,
                                            get_NATUREZA_treated, get_Flux_Cli_treated, get_TP_ESTAB_treated,
                                            get_TurnosAt_treated, get_NIV_HIER_treated, get_TIPOPRES_treated,
                                            get_ORGEXPED_treated, get_CLASAVAL_treated, get_NATJUR_treated)

from .data_wrangling.prepare_CNES_PF import get_CBO_treated, get_CR_CONSEL_treated, get_VINCULO_treated

from .data_wrangling.prepare_CNES_LT import get_tip1leit_treated, get_Esp_leit_treated

from .data_wrangling.prepare_CNES_EQ import get_TP_EQUIPAM_treated, get_Equip_Tp_treated

from .data_wrangling.prepare_CNES_SR import get_SERVICO_treated, get_CLASSSR_treated, get_Srv_Caract_treated

from .data_wrangling.prepare_CNES_EP import (get_EQP_XX_treated, get_EQUIPE_treated, get_AREA_XX_treated,
                                            get_SEGM_XX_treated, get_tiposegm_treated, get_motdesat_treated,
                                            get_TP_DESAT_treated)

from .data_wrangling.prepare_CNES_HB import get_HABILITA_treated

from .data_wrangling.prepare_CNES_RC import get_REGRAS_treated

from .data_wrangling.prepare_CNES_GM import get_GESTAO_treated

from .data_wrangling.prepare_CNES_EE import get_ESTABENS_treated

from .data_wrangling.prepare_CNES_EF import get_ESTABFIL_treated


############################################################################################################################################################################
#  pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
############################################################################################################################################################################
############################################################################################################################################################################
# AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES * AUXILIARY TABLES #
############################################################################################################################################################################

# Função que utiliza para a inserção de dados não principais o pandas.to_sql + SQLAlchemy
def insert_most_CNES_tables_pandas(path, device, child_db):
    # Inserção dos dados das tabelas não principais no banco de dados
    label1 = 'append'
    label2 = 'ID'

    # Chama funções definidas no módulo "prepare_CNES_ST" do package "data_wrangling"
    df_CADGERBR = get_CADGERBR_treated(path)
    df_CADGERBR.to_sql('cnes', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TABUF = get_TABUF_treated()
    df_TABUF.to_sql('ufcod', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CADMUN = get_CADMUN_treated()
    df_CADMUN.to_sql('codufmun', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_PFPJ = get_TP_PFPJ_treated()
    df_TP_PFPJ.to_sql('pfpj', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NIVELDEP = get_NIVELDEP_treated()
    df_NIVELDEP.to_sql('nivdep', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_RETENMAN = get_RETENMAN_treated()
    df_RETENMAN.to_sql('codir', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TPGESTAO = get_TPGESTAO_treated()
    df_TPGESTAO.to_sql('tpgestao', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_EsferAdm = get_EsferAdm_treated()
    df_EsferAdm.to_sql('esferaa', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Ativ_Ens = get_Ativ_Ens_treated()
    df_Ativ_Ens.to_sql('atividad', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATUREZA = get_NATUREZA_treated()
    df_NATUREZA.to_sql('natureza', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Flux_Cli = get_Flux_Cli_treated()
    df_Flux_Cli.to_sql('clientel', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TP_ESTAB = get_TP_ESTAB_treated()
    df_TP_ESTAB.to_sql('tpunid', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TurnosAt = get_TurnosAt_treated()
    df_TurnosAt.to_sql('turnoat', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NIV_HIER = get_NIV_HIER_treated()
    df_NIV_HIER.to_sql('nivhier', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_TIPOPRES = get_TIPOPRES_treated()
    df_TIPOPRES.to_sql('tpprest', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_ORGEXPED = get_ORGEXPED_treated()
    df_ORGEXPED.to_sql('orgexped', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CLASAVAL = get_CLASAVAL_treated()
    df_CLASAVAL.to_sql('clasaval', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_NATJUR = get_NATJUR_treated()
    df_NATJUR.to_sql('natjur', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_PF" do package "data_wrangling"
    df_CBO = get_CBO_treated(path)
    df_CBO.to_sql('cbo', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CBO.to_sql('cbounico', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CR_CONSEL = get_CR_CONSEL_treated()
    df_CR_CONSEL.to_sql('conselho', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_VINCULO = get_VINCULO_treated()
    df_VINCULO.to_sql('vinculac', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_LT" do package "data_wrangling"
    df_tip1leit = get_tip1leit_treated()
    df_tip1leit.to_sql('tpleito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Esp_leit = get_Esp_leit_treated()
    df_Esp_leit.to_sql('codleito', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_EQ" do package "data_wrangling"
    df_TP_EQUIPAM = get_TP_EQUIPAM_treated()
    df_TP_EQUIPAM.to_sql('tipequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Equip_Tp = get_Equip_Tp_treated()
    df_Equip_Tp.to_sql('codequip', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_SR" do package "data_wrangling"
    df_SERVICO = get_SERVICO_treated()
    df_SERVICO.to_sql('servesp', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_CLASSSR = get_CLASSSR_treated()
    df_CLASSSR.to_sql('classsr', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_SERVICO.to_sql('srvunico', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    df_Srv_Caract = get_Srv_Caract_treated()
    df_Srv_Caract.to_sql('caracter', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_HB" do package "data_wrangling"
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

    # Chama funções definidas no módulo "prepare_CNES_HB" do package "data_wrangling"
    df_HABILITA = get_HABILITA_treated()
    df_HABILITA.to_sql('sgruphab_hb', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_RC" do package "data_wrangling"
    df_REGRAS = get_REGRAS_treated()
    df_REGRAS.to_sql('sgruphab_rc', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_GM" do package "data_wrangling"
    df_GESTAO = get_GESTAO_treated()
    df_GESTAO.to_sql('sgruphab_gm', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_EE" do package "data_wrangling"
    df_ESTABENS = get_ESTABENS_treated()
    df_ESTABENS.to_sql('sgruphab_ee', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)

    # Chama funções definidas no módulo "prepare_CNES_EF" do package "data_wrangling"
    df_ESTABFIL = get_ESTABFIL_treated()
    df_ESTABFIL.to_sql('sgruphab_ef', con=device, schema=child_db, if_exists=label1, index=False, index_label=label2)


###########################################################################################################################################################################
# pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas pandas #
###########################################################################################################################################################################
###########################################################################################################################################################################
#  MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE * MAIN TABLE #
###########################################################################################################################################################################

# Função que utiliza para a inserção de dados principais da "child_db" o pandas.to_sql + SQLAlchemy
def insert_main_tables_e_files_info_pandas(file_name, directory, date_ftp, device, child_db, parent_db):
    start = time.time()
    counting_rows = pd.read_sql('''SELECT COUNT('NOME') FROM %s.arquivos''' % (child_db), con=device)
    qtd_files_pg = counting_rows.iloc[0]['count']
    print(f'A quantidade de arquivos principais de dados do {child_db} já carregada no {parent_db}/PostgreSQL é {qtd_files_pg}.')

    # Tratamento de dados principais do CNES
    base = file_name[0:2]
    state = file_name[2:4]
    year = file_name[4:6]
    month = file_name[6:8]
    main_table = file_name[0:2].lower() + 'br'
    counting_rows = pd.read_sql('''SELECT COUNT(*) from %s.%s''' % (child_db, main_table), con=device)
    n_rows = counting_rows.iloc[0]['count']
    print(f'\nIniciando a lida com o arquivo {base}{state}{year}{month}.')

    # Criação de objeto string do nome de uma função de tratamento de dados de uma tabela principal...
    # do "child_db" contida no respectivo módulo do package "data_wrangling"
    func_string = 'get_' + file_name[0:2] + 'XXaamm_treated'

    # Importação da função de tratamento de dados de uma tabela principal do "child_db" usando a função python "__import__"
    module = __import__('insertion.data_wrangling.prepare_CNES_' + base, fromlist=[func_string], level=0)
    func_treatment = getattr(module, func_string)

    # Chama a função "func_treatment" do módulo "prepare_CNES_XX" do package "data_wrangling"
    df = func_treatment(state, year, month)
    # Inserção das colunas UF_XX, ANO_XX e MES_XX no objeto pandas DataFrame "df"
    df.insert(1, 'UF_' + base, [state]*df.shape[0])
    df.insert(2, 'ANO_' + base, [int('20' + year)]*df.shape[0])
    df.insert(3, 'MES_' + base, [month]*df.shape[0])
    df['CONTAGEM'] = np.arange(n_rows + 1, n_rows + 1 + df.shape[0])
    # Inserção dos dados da tabela principal no banco de dados "child_db"
    df.to_sql(main_table, con=device, schema=child_db, if_exists='append', index=False)
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
    print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {parent_db}/PostgreSQL pelo pandas!')

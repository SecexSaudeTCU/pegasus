###########################################################################################################################
# ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT #
###########################################################################################################################

import os
import datetime
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import traceback

from utilities.essential_postgreSQL import files_in_ftp_subbase, get_tables_counts_subdb, files_loaded, files_to_load

ARQUIVOS_NAO_CARREGADOS = []

def load_any(db_name, db_user, db_password, datasus_db, first_year, last_year, exceptions):
   
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Nome do banco de dados do Datasus almejado
 
    print('\nTo its goal...\n')

    # Criação de objetos string do nome das duas funções de inserção de dados do "datasus_db" contidas no respectivo...
    # módulo do package "insertion"
    str_most = 'insert_into_most_' + datasus_db.upper() + '_tables'
    str_main = 'insert_into_main_table_and_arquivos'

    # Importação das duas funções de inserção de dados do "datasus_db" usando a função nativa "__import__"
    if datasus_db.startswith('sinan'):
        #
        module1 = __import__('insertion.insert_into_any_' + datasus_db.upper()[:-5],
                             fromlist=[str_most, str_main],
                             level=0)
    else:
        #
        module1 = __import__('insertion.insert_into_any_' + datasus_db.upper()[:-3],
                             fromlist=[str_most, str_main],
                             level=0)

    # Colocação do nome das duas funções de inserção de dados importadas nas variáveis "most_tables" e "main_table"
    most_tables = getattr(module1, str_most)
    main_table = getattr(module1, str_main)

    # Chama a função "files_in_ftp_subbase" contida no módulo "essential_postgreSQL" do package "utilities" tendo...
    # como parâmetro a variável "datasus_db"
    df_arquivos_ftp = files_in_ftp_subbase(datasus_db)
    os.remove('stuff_ftp_files.txt')

    # Dados de conexão 1 (portanto o DB_NAME já deve ter sido previamente criado com esses dados)
    DB_NAME = db_name       # De acordo com o usuário; Nome do banco de dados mãe
    DB_HOST = '127.0.0.1'
    DB_PORT = '5432'
    DB_USER = db_user       # De acordo com o usuário
    DB_PASS = db_password   # De acordo com o usuário
    # Criação de objeto ndarray que armazena os referidos objetos string
    DB_DADOS = np.array([DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASS])

    try:
        # Conecta ao banco de dados mãe "DB_NAME" do SGBD PostgreSQL usando o módulo python "psycopg2"
        conn = psycopg2.connect(dbname=DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
        print(conn.get_dsn_parameters(),'\n')
        # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
        cursor = conn.cursor()
        # Inicializa o schema denominado "datasus_db" no banco de dados mãe "DB_NAME"
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {datasus_db};')
        conn.commit()
        # Chama a função "get_tables_counts_subdb" contida no módulo "essential_postgreSQL" do package "utilities"
        dict_tabelas_e_counts_pg = get_tables_counts_subdb(cursor, datasus_db)
        # Encerra o cursor
        cursor.close()
        # Encerra a conexão
        conn.close()
    except (Exception, psycopg2.Error) as error:
        print('Erro ao conectar ao banco de dados PostgreSQL.', error)

    # Importação da função "create_tables" de criação do schema do banco de dados "datasus_db" existente no respectivo
    # módulo do package "schemas" usando a função python "__import__"
    module2 = __import__('schemas.sql_' + datasus_db.upper() + '_postgreSQL',
                         fromlist=['create_tables'],
                         level=0)
    # Referenciação da função "create_tables" à variável deste módulo denominada "create_schema"
    create_schema = getattr(module2, 'create_tables')
    # Uso da função "create_schema" para a criação do schema do banco de dados "datasus_db"
    create_schema(DB_DADOS, datasus_db)

    # Dados de conexão 2 (para uso da função "create_engine" do SQLAlchemy)
    DB_TYPE = 'postgresql'
    DB_DRIVER = 'psycopg2'
    # URI do banco de dados mãe "DB_NAME" do SGBD PostgreSQL
    DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
    # Cria um "engine" para o banco de dados mãe "DB_NAME" usando a função "create_engine" do SQLAlchemy
    engine = create_engine(DATABASE_URI)

    # Chama a função "files_loaded" contida no módulo "essential_postgreSQL" do package "utilities"
    df_arquivos_pg = files_loaded(dict_tabelas_e_counts_pg, datasus_db, engine)
    print(df_arquivos_pg)

    # Chama a função "files_to_load" contida no módulo "essential_postgreSQL" do package "utilities"
    df_arqs_nao_carregados = files_to_load(df_arquivos_ftp, df_arquivos_pg, datasus_db, first_year, last_year)

    # Quantidade de arquivos de dados principal que falta carregar em "datasus_db"
    qtd_arqs_datasus = df_arqs_nao_carregados.shape[0]
    print(f'\nA quantidade de arquivos principais de dados do {datasus_db} que falta carregar no {DB_NAME}/PostgreSQL é {qtd_arqs_datasus}.\n')

    # Cria o objeto string "path_xlsx" do diretório onde estão os eventuais arquivos "xlsx" relativos a "datasus_db"
    CURR_DIR = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
    if datasus_db.startswith('cnes'):
        path_xlsx = CURR_DIR + '/files/CNES/'
    elif datasus_db.startswith('sih'):
        path_xlsx = CURR_DIR + '/files/SIH/'
    elif datasus_db.startswith('sia'):
        path_xlsx = CURR_DIR + '/files/SIA/'
    elif datasus_db.startswith('sinan'):
        path_xlsx = CURR_DIR + '/files/SINAN/'

    # Remove a única key do objeto dict "dict_tabelas_e_counts_pg" que não se refere às parent tables do "datasus_db"...
    # utilizando o método "pop" da class "dict"
    dict_tabelas_e_counts_pg.pop('arquivos', None)
    # Inicialização de contador do número de parent tables do "datasus_db" sem dados
    count = 0
    # Faz a contagem de quantas parent tables do "datasus_db" estão sem dados, se é que estão
    for table in dict_tabelas_e_counts_pg:
        if dict_tabelas_e_counts_pg[table] == 0:
            count += 1

    # Carrega dados de tabela(s) auxiliar(es) (parent table(s)) do "datasus_db" se não constar dados dela(s) no...
    # "DBNAME" do PostgreSQL
    if ((dict_tabelas_e_counts_pg == dict()) or (count > 0)):
        print(f'\nIniciando a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando pandas...')
        # Chama a função "most_tables" para inserção das tabelas auxiliares (parent tables) no "datasus_db" pelo...
        # método pandas.to_sql
        try:   
            most_tables(path_xlsx, engine, datasus_db)
        except Exception as e:
            print("ERRO IDENTIFICADO E IGNORADO")
            pilha_erros = traceback.format_exc()
            exceptions.append('\n' + '='*60 + '\n' + '='*60 + '\n' + pilha_erros + '='*60)
                        
        print(f'Finalizou a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL.')

        # Remoção de pastas vazias que contiveram arquivos "dbf" e/ou "cnv"
        if datasus_db in np.array(['cnes_st', 'cnes_lt', 'cnes_eq', 'cnes_sr', 'cnes_ep', 'cnes_ee', 'cnes_ef', 'cnes_in']):
            
           os.remove('TAB_CNES.zip') if os.path.isfile('TAB_CNES.zip') else 0 
           os.rmdir('DBF') if os.path.isdir('DBF') else 0
           os.rmdir('cnv') if os.path.isdir('cnv') else 0

        elif datasus_db in np.array(['sih_rd', 'sih_sp']):
            os.remove('TAB_SIH.zip') if os.path.isfile('TAB_SIH.zip') else 0 
            os.rmdir('CNV') if os.path.isdir('CNV') else 0
            os.rmdir('DBF') if os.path.isdir('DBF') else 0

        elif datasus_db in np.array(['sia_pa']):
            os.remove('TAB_SIA.zip') if os.path.isfile('TAB_SIA.zip') else 0 
            os.remove('TAB_SIH.zip') if os.path.isfile('TAB_SIH.zip') else 0 
            os.rmdir('CNV') if os.path.isdir('CNV') else 0
            os.rmdir('TAB_DBF') if os.path.isdir('CNV') else 0

        elif datasus_db == 'sinan_deng':
            os.remove('TAB_SINAN.zip')

        os.remove('base_territorial.zip')

    print(f'\nIniciando a inserção de dados principais no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando copy_expert...')
    # Carrega dados da tabela principal do "datasus_db" no PostgreSQL
    
    for i in range(qtd_arqs_datasus):
        # Chama a função "main_table" para a inserção de dados na tabela principal (child table) + respectivas informações...
        # na tabela arquivos usando os métodos copy_expert + pandas.to_sql
        try:
            main_table(df_arqs_nao_carregados.NOME[i], df_arqs_nao_carregados.DIRETORIO[i],
                   df_arqs_nao_carregados.DATA_INSERCAO_FTP[i], engine, datasus_db, DB_DADOS)
        except Exception as e:
            ARQUIVOS_NAO_CARREGADOS.append(df_arqs_nao_carregados.NOME[i])
            print("ERRO IDENTIFICADO E IGNORADO")
            pilha_erros = traceback.format_exc()
            exceptions.append('\n' + '='*60 + '\n' + '='*60 + '\n' + pilha_erros + '='*60)
            print(str(e))
            pass
    if len(exceptions) > 0:
        with open('./logs/erros_internos/{}_{}_{}.txt'.format(datasus_db, last_year,str(datetime.datetime.now())[:19].replace(':','_')), "w") as log_error:
            log_error.write(" ".join(exceptions))


###########################################################################################################################
# MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN #
###########################################################################################################################
DATABASES = {
    'CNES' : ['ST','PF','LT','EQ', 'SR','EP','HB','GM','EE','EE','EF','IN','RC'],
    'SIH' :  ['RD', 'SP'],
    'SIA' :  ['PA'],
    'SINAN' : ['DENG']
    }
ANOS = [2021]
   
if __name__ == '__main__':

    print('\n*******************************************************************')
    print('O banco de dados PostgreSQL a ser preenchido deve antes ser criado!'.upper())
    print('*******************************************************************\n')

    decision = input("Digite 'q' se o banco de dados ainda não foi criado ou qualquer outra tecla em caso contrário: ")
    if decision == 'q':
        quit()
    else:
        for db in DATABASES:
            
            for sub_db in DATABASES[db]:
                if sub_db == 'PA':
                    continue
                else:
                    datasus_db = (db + '_' + sub_db).lower()
                    for ano in ANOS:
                        print('EXECUTANDO PARA O BANCO {} OS DADOS DO ANO {}'.format(datasus_db, ano))
                        try:
                            exceptions = []
                            load_any('dbsus4', 'postgres', '123456', datasus_db,ano, ano, exceptions)
                        except:
                            pilha_erros = traceback.format_exc()
                            exceptions.append('\n' + '='*60 + '\n' + '='*60 + '\n' + pilha_erros + '='*60)
                            
                            with open('./logs/erros_gerais/{}_{}_{}.txt'.format(
                                            datasus_db, ano,str(datetime.datetime.now())[:19].replace(
                                                ':','_')), "w") as log_error:

                                log_error.write(" ".join(exceptions))
###########################################################################################################################
# ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT #
###########################################################################################################################

import os

import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd

from utilities.essential_postgreSQL import files_in_ftp_subbase, get_tables_counts_subdb, files_loaded, files_to_load


def load_any(db_name, db_user, db_password):
    """
    Cria um schema do banco de dados "db_name" no SGBD PostgreSQL sem integridade referencial (de "primary and foreign
    keys") e insere nele dados das bases de dados (do Datasus) CNES, SIH, SIA, SINAN ou XXX no SGBD PostgreSQL pelo
    método copy_expert da classe cursor do pacote psycopg. Os dados são inseridos por sub-base de dados: CNES_ST, CNES_DC,
    CNES_PF, CNES_LT, CNES_EQ, CNES_SR, CNES_EP, CNES_HB, CNES_RC, CNES_GM, CNES_EE, CNES_EF, CNES_IN, SIH_RD, SIH_SP,
    SIA_PA, SINAN_DENG ou XXX. Assim, para cada sub-base de dados do Datasus é criado um schema.

    A inserção de dados consiste dos arquivos principais de dados em formato "dbc" e dos arquivos auxiliares de dados
    em formato "dbf", "cnv" e "xlsx" das sub-bases de dados CNES_ST (STXXaamm), CNES_DC (DCXXaamm), CNES_PF (PFXXaamm),
    CNES_LT (LTXXaamm), CNES_EQ (EQXXaamm), CNES_SR (SRXXaamm), CNES_EP (EPXXaamm), CNES_HB (HBXXaamm), CNES_RC (RCXXaamm),
    CNES_GM (GMXXaamm), CNES_EE (EEXXaamm), CNES_EF (EFXXaamm), CNES_IN (INXXaamm), SIH_RD (RDXXaamm), SIH_SP (SPXXaamm),
    SIA_PA (PAXXaamm), SINAN_DENG (DENGXXaa) ou XXX. Destaca-se que algumas bases de dados, como a do CNES, se subdividem
    em várias tabelas principais de dados, que no caso são em número de 13, conforme se pode contabilizar acima.

    Os arquivos principais de dados formam a tabela principal (child table) das respectiva sub-base de dados e estão em
    pastas específicas do endereço ftp do Datasus (ftp://ftp.datasus.gov.br/dissemin/publicos/) em formato "dbc". Cada
    arquivo "dbc" é baixado em tempo de execução, descompactado para "dbf", lido como um objeto pandas DataFrame e, para
    evitar a repetição do download, é salvo numa pasta criada dinamicamente e denominada "datasus_content" no computador
    de execução deste módulo no formato "parquet" no caso de nova necessidade desse arquivo principal de dados.

    Os arquivos secundários de dados formam as tabelas relacionais (parent tables) à tabela principal e estão em formato
    "dbf", "cnv" ou "xlsx". Os arquivos "dbf" e "cnv", presentes em diretórios do endereço ftp do Datasus, são baixados
    e convertidos em tempo de execução para objetos pandas DataFrame enquanto os arquivos "xlsx", quando necessários,
    foram  criados a partir de relações descritas no Dicionário de Dados da respectiva base de dado do Datasus e não
    retratadas em arquivos  "dbf" ou "cnv" ou a partir da incompletude de arquivos "dbf" ou "cnv".

    O referido diretório "datasus_content" onde são baixados os arquivos principais de dados em formato "parquet" pode
    ser alterado editando o módulo "folder" contido no subpackage "pegasus.dados.transform.extract".
    """

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    print('\n*******************************************************************')
    print('O banco de dados PostgreSQL a ser preenchido deve antes ser criado!'.upper())
    print('*******************************************************************\n')

    decision = input("Digite 'q' se o banco de dados ainda não foi criado ou qualquer outra tecla em caso contrário: ")
    if decision == 'q':
        quit()
    else:
        # Nome do banco de dados do Datasus almejado
        datasus_db = input('\nDigite o nome da base de dados do Datasus (CNES/SIH/SIA/SINAN/XXX): ').lower()

        # Construção do objeto string do nome do sub-banco de dados do CNES, SIH, SIA, SINAN ou XXX
        # CNES
        if datasus_db == 'cnes':
            print('\nST: Estabelecimentos')
            print('DC: Dados Complementares')
            print('PF: Profissionais')
            print('LT: Leitos')
            print('EQ: Equipamentos')
            print('SR: Serviços Especializados')
            print('EP: Equipes')
            print('HB: Habilitações')
            print('GM: Gestão e Metas')
            print('EE: Estabelecimentos de Ensino')
            print('EF: Estabelecimentos Filantrópicos')
            print('IN: Incentivos')
            print('RC: Regras Contratuais')
            sub_db = input('\nDigite as iniciais da sub-base de dados do CNES (ST/DC/PF/LT/EQ/SR/EP/HB/RC/GM/EE/EF/IN): ').lower()
        # SIH
        elif datasus_db == 'sih':
            print('\nRD: Autorização de Internação Hospitalar - Reduzidas')
            print('SP: Autorização de Internação Hospitalar - Serviços Profissionais')
            sub_db = input('\nDigite as iniciais da sub-base de dados do SIH (RD/SP): ').lower()
        # SIA
        elif datasus_db == 'sia':
            print('\nPA: Procedimentos Ambulatoriais')
            sub_db = input('Digite as iniciais da sub-base de dados do SIA (PA/XX/.../YY): ').lower()
        # SINAN
        elif datasus_db == 'sinan':
            print('\nDENG: Dengue e Chikungunya\n')
            sub_db = input('Digite as iniciais da sub-base de dados do SINAN (DENG/XXXX/.../YYYY): ').lower()
        datasus_db += '_' + sub_db
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
        df_arqs_nao_carregados = files_to_load(df_arquivos_ftp, df_arquivos_pg)

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
        elif datasus_db == 'sim':
            path_xlsx = CURR_DIR + '/files/SIM/'
        elif datasus_db == 'sinasc':
            path_xlsx = CURR_DIR + '/files/SINASC/'
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
            most_tables(path_xlsx, engine, datasus_db)
            print(f'Finalizou a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL.')

            # Remoção de pastas vazias que contiveram arquivos "dbf" e/ou "cnv"
            if datasus_db in np.array(['cnes_st', 'cnes_lt', 'cnes_eq', 'cnes_sr', 'cnes_ep', 'cnes_ee', 'cnes_ef', 'cnes_in']):
                os.remove('TAB_CNES.zip')
                os.rmdir('TAB_DBF')
                os.rmdir('cnv')
            elif datasus_db in np.array(['sih_rd', 'sih_sp']):
                os.remove('TAB_SIH.zip')
                os.rmdir('CNV')
                os.rmdir('DBF')
            elif datasus_db in np.array(['sia_pa']):
                os.remove('TAB_SIA.zip')
                os.remove('TAB_SIH.zip')
                os.rmdir('CNV')
                os.rmdir('TAB_DBF')
            elif datasus_db == 'sim':
                os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDO18.dbf
            elif datasus_db == 'sinasc':
                os.remove('NASC_NOV_TAB.zip')
                os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDN18.dbf
            elif datasus_db == 'sinan_deng':
                os.remove('TAB_SINAN.zip')
            os.remove('base_territorial.zip')

        print(f'\nIniciando a inserção de dados principais no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando copy_expert...')
        # Carrega dados da tabela principal do "datasus_db" no PostgreSQL
        for i in range(qtd_arqs_datasus):
            # Chama a função "main_table" para a inserção de dados na tabela principal (child table) + respectivas informações...
            # na tabela arquivos usando os métodos copy_expert + pandas.to_sql
            main_table(df_arqs_nao_carregados.NOME[i], df_arqs_nao_carregados.DIRETORIO[i],
                       df_arqs_nao_carregados.DATA_INSERCAO_FTP[i], engine, datasus_db, DB_DADOS)


###########################################################################################################################
# MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN #
###########################################################################################################################

if __name__ == '__main__':

    load_any('dbsus4', 'ericc', 'teste')

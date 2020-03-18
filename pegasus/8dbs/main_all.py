########################################################################################################################################################################
# ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT #
########################################################################################################################################################################

import os

import psycopg2
from sqlalchemy import create_engine, event, DDL
import pandas as pd
import numpy as np

from utilities.essential_postgreSQL_db_sus import files_in_ftp, get_tables_e_count_postgreSQL, files_in_postgreSQL, difference_files

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


"""
Cria um schema do banco de dados (neste computador denominado) "dbsus" no SGBD PostgreSQL com integridade referencial
(de "primary and foreign keys") e insere neles dados dos sistemas (do Datasus) CNES, SIH, SIM, SINASC ou XXX no SGBD
PostgreSQL pelo método pandas.to_sql. Os dados são inseridos por sistema: CNES, SIH, SIM, SINASC ou XXX. Assim, para
cada sistema do Datasus é criado um único schema.

A inserção de dados consiste dos arquivos principais de dados em formato "dbc" e dos arquivos secundários de dados em
formato "dbf", "cnv" e "xlsx" dos sistemas CNES (STXXaamm + DCXXaamm + PFXXaamm + LTXXaamm + EQXXaamm + SRXXaamm +...
EPXXaamm + HBXXaamm + RCXXaamm + GMXXaamm + EEXXaamm + EFXXaamm + INXXaamm), SIH (RDXXaamm + SPXXaamm), SIM (DOXXaaaa),
SINASC (DNXXaaaa) e XXX. Destaca-se que alguns sistemas, como o CNES, se subdividem em várias tabelas principais de dados,
que no caso são em número de 13, conforme se pode contabilizar acima.

Os arquivos principais de dados formam a tabela principal (child table) do respectivo sistema ou subsistema e estão em
pastas específicas do endereço ftp do Datasus (ftp://ftp.datasus.gov.br/dissemin/publicos/) em formato "dbc". Cada arquivo
"dbc" é baixado em tempo de execução, descompactado para "dbf", lido como um objeto pandas DataFrame e, para evitar a
repetição do download, é salvo numa pasta criada dinamicamente e denominada "datasus_content" no computador de execução deste
script no formato "parquet" no caso de nova necessidade desse arquivo principal de dados.

Os arquivos secundários de dados formam as tabelas relacionais (parent tables) à tabela principal e estão em formato "dbf",
"cnv" ou "xlsx". Os arquivos "dbf" e "cnv", presentes em diretórios do endereço ftp do Datasus, são baixados e convertidos
em tempo de execução para objetos pandas DataFrame enquanto os arquivos "xlsx", quando necessários, foram criados a partir
de relações descritas no Dicionário de Dados do respectivo sistema do Datasus e não retratadas em arquivos "dbf" ou "cnv"
ou a partir da incompletude de arquivos "dbf" ou "cnv".

É necessário instalar o SGBD PostgreSQL (https://www.postgresql.org/download/) e uma plataforma para gerenciamento de banco
de dados é recomendável ter, tal como pgAdmin (https://www.pgadmin.org/download/) ou DBeaver [Community] (https://dbeaver.io/).
É necessário ter as seguintes bibliotecas Python instaladas: psycopg2, SQLAlchemy, ftplib, zipfile, dbfread, xlrd, pyarrow,
fast_parquet, numpy e pandas.

Para se executar esse pacote em Python 3.7.4 a partir do sistema operacional Windows 10 também se instalou em C:/ o programa
TabWin do Datasus, que no seu diretório raiz contém um executável que permite a conversão de arquivos em formato "dbc" para
"dbf" denominado "dbf2dbc". O programa TabWin pode ser baixado de http://datasus1.saude.gov.br/transferencia-download...
-de-arquivos/download-do-tabwin selecionando o link "Tab415.zip" presente na primeira linha da coluna "Nome" da tabela que
aparece nessa página. As instruções de instalação presentes nessa página ensinam:

"Os arquivos compactados abaixo contêm os componentes básicos que permitem o funcionamento do Tab para Windows.
Sugerimos que você crie uma pasta, em seu computador, chamada TabWin, e copie o arquivo abaixo para essa pasta."

Por outro lado, para executar esse pacote em Python 3.6.9 a partir do sistema operacional Linux também se instalou a
dependência libffi ("$ sudo apt install libffi-dev") e as bibliotecas Python codecs e cffi, prescindindo, porém, da
instalação do executável "dbf2dbc". Nesse caso, se deve executar o módulo "_build_readdbc" contido no sub-package
"insertion.data_wrangling.online" para criar um wrapper de um módulo na linguagem C que descompacta arquivos "dbc" para
"dbf". Ainda se está trabalhando na coexistência Windows e Linux.

O referido diretório "datasus_content" onde são baixados os arquivos principais de dados em formato "parquet" pode ser
alterado editando o módulo "folder" contido no sub-package "insertion.data_wrangling.online".

"""


########################################################################################################################################################################
# MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN #
########################################################################################################################################################################

if __name__ == '__main__':
    print('\n***************************************************************************************')
    print('First of all, you must read the docstring provided at the beginning of this main module.\n'.upper())
    print('Also, you just do not forget that the PostgreSQL database to be filled must first be created!'.upper())
    print('Therefore the 3 constants defined at lines 125, 126 and 129 of this main module must...')
    print('first be adapted to your reality, that is: DB_USER, DB_PASS and DB_NAME; being DB_NAME...')
    print('the name of the database created in PostgreSQL.')
    print('***************************************************************************************\n')

    decision = input("You better really type 'q' if you have not done the recommended above or type anything otherwise: ")
    if decision == 'q':
        quit()
    else:
        pass

    # Nome do banco de dados do Datasus almejado
    datasus_db = input('\nEnter the Datasus database name (CNES/SIH/SIA/SIM/SINASC/SINAN/XXX/YYY): ').lower()
    if datasus_db == 'sia':
        print('\nUse the module "main_any.py" and it is still only available sia_pa!')
        quit()
    elif datasus_db == 'sinan':
        print('\nUse the module "main_any.py" and it is still only available sinan_deng!')
        quit()
    else:
        print('\nTo its goal...\n')

    # Importação da variável "Base" do respectivo módulo do "datasus_db" do package "schemas"
    module1 = __import__('schemas.alchemy_declarative_' + datasus_db.upper() + '_postgreSQL', fromlist=['Base'], level=0)
    structure = module1.Base

    # Cria o schema do banco de dados child "datasus_db" do banco de dados mãe "DB_NAME" utilizando as classes definidas...
    # no respectivo módulo do package "schemas"
    event.listen(structure.metadata, 'before_create', DDL('CREATE SCHEMA IF NOT EXISTS %s' % (datasus_db)))

    # Criação de objetos string do nome das duas funções de inserção de dados do "datasus_db" contidas no respectivo...
    # módulo do package "insertion"
    str_most = 'insert_most_' + datasus_db.upper() + '_tables_pandas'
    str_main = 'insert_main_table_e_file_info_pandas'

    # Importação das duas funções de inserção de dados do "datasus_db" usando a função nativa "__import__"
    module2 = __import__('insertion.insert_into_all_' + datasus_db.upper(), fromlist=[str_most, str_main], level=0)

    # Colocação do nome das duas funções de inserção de dados importadas nas variáveis "most_tables" e "main_tables"
    most_tables = getattr(module2, str_most)
    main_tables = getattr(module2, str_main)

    # Chama a função "files_in_ftp" contida no módulo "essential_postgreSQL_db_sus" do package "utilities" tendo como parâmetro...
    # a variável "datasus_db"
    df_arquivos_ftp = files_in_ftp(datasus_db)
    os.remove('stuff_ftp_files.txt')

    # Dados de conexão 1 (portanto o DB_NAME já deve ter sido previamente criado com esses dados)
    DB_TYPE = 'postgresql'
    DB_USER = 'Eric'
    DB_PASS = 'teste'
    DB_HOST = '127.0.0.1'
    DB_PORT = '5432'
    DB_NAME = 'dbsus'     # Nome no PostgreSQL do banco de dados mãe agregador dos bancos de dados do Datasus

    try:
        # Conecta ao SGBD PostgreSQL usando o módulo python "psycopg2"
        conn = psycopg2.connect(dbname=DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
        print(conn.get_dsn_parameters(),'\n')
        # Criação de um cursor da conecção tipo "psycopg2" referenciado à variável "cursor"
        cursor = conn.cursor()
        # Chama a função "get_tables_e_count_postgreSQL" contida no módulo "essential_postgreSQL_db_sus" do package "utilities"
        dict_tabelas_e_counts_pg = get_tables_e_count_postgreSQL(cursor, datasus_db)
        conn.close()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL.', error)

    # Dados de conexão 2
    DB_DRIVER = 'psycopg2'

    # URI para o PostgreSQL
    DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)

    # Cria um "engine" para o SGBD PostgreSQL usando o driver "psycopg2"
    engine = create_engine(DATABASE_URI)
    # Vincula o schema de "structure" definido no módulo "alchemy_declarative_'datasus_db'_postgreSQL" do package "schemas" ao banco de dados "DBNAME"
    structure.metadata.create_all(engine)

    # Chama a função "files_in_postgreSQL" contida no módulo "essential_postgreSQL_db_sus" do package "utilities"
    df_arquivos_pg = files_in_postgreSQL(dict_tabelas_e_counts_pg, datasus_db, engine)
    print(df_arquivos_pg)

    # Chama a função "difference_files" contida no módulo "essential_postgreSQL_db_sus" do package "utilities"
    df_arqs_nao_carregados = difference_files(df_arquivos_ftp, df_arquivos_pg)

    # Quantidade de arquivos principais de dados que falta carregar em "datasus_db"
    qtd_arqs_datasus = df_arqs_nao_carregados.shape[0]
    print(f'\nA quantidade de arquivos principais de dados do {datasus_db} que falta carregar no {DB_NAME}/PostgreSQL é {qtd_arqs_datasus}.\n')

    # Cria o objeto string "path_xlsx" do diretório onde estão os eventuais arquivos "xlsx" relativos a "datasus_db"
    path_xlsx = os.getcwd() + '/files/' + datasus_db.upper() + '/'

    # Remove a única key do objeto dict "dict_tabelas_e_counts_pg" que não se refere às parent tables do "datasus_db" utilizando...
    # o método "pop" da class "dict"
    dict_tabelas_e_counts_pg.pop('arquivos', None)
    # Inicialização de contador do número de parent tables do "datasus_db" sem dados
    count = 0
    # Faz a contagem de quantas parent tables do "datasus_db" estão sem dados, se é que estão
    for table in dict_tabelas_e_counts_pg:
        if dict_tabelas_e_counts_pg[table] == 0:
            count += 1

    # Carrega dados de tabela(s) auxiliar(es) (parent table(s)) do "datasus_db" se não constar dados dela(s) no "DBNAME" do PostgreSQL
    if ((dict_tabelas_e_counts_pg == dict()) or (count > 0)):
        print(f'\nIniciando a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando pandas...')
        # Chama a função "most_tables" para inserção das tabelas auxiliares (parent tables) no "datasus_db"
        most_tables(path_xlsx, engine, datasus_db)
        print(f'Finalizou a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando pandas.')

        # Remoção dos arquivos "cnv" baixados numa pasta zipada do endereço ftp do Datasus
        if datasus_db == 'cnes':
            os.remove('TAB_CNES.zip')
            os.rmdir('TAB_DBF_CNV')
        elif datasus_db == 'sih':
            os.remove('TAB_SIH.zip')
            os.rmdir('CNV')
            os.rmdir('DBF')
        elif datasus_db == 'sia':
            s.remove('TAB_SIA.zip')
            os.remove('TAB_SIH.zip')
            os.rmdir('CNV')
            os.rmdir('TAB_DBF')
        elif datasus_db == 'sim':
            os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDO18.dbf
        elif datasus_db == 'sinasc':
            os.remove('NASC_NOV_TAB.zip')
            os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDN18.dbf
        elif datasus_db == 'sinan':
            os.remove('TAB_SINAN.zip')

    # Carrega dados da tabela principal do "datasus_db" no PostgreSQL
    for i in range(qtd_arqs_datasus):
        # Chama a função "main_tables" para a inserção de dados na tabela principal (child table) + respectivas informações na tabela arquivos...
        # usando o pandas.to_sql "do" SQLAlchemy
        main_tables(df_arqs_nao_carregados.NOME[i], df_arqs_nao_carregados.DIRETORIO[i], df_arqs_nao_carregados.DATA_INSERCAO_FTP[i],
                    engine, datasus_db, DB_NAME)
